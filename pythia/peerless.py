from datetime import date, timedelta
import logging
import multiprocessing as mp
import os
import queue
import threading
import pythia.functions
import pythia.io
import pythia.template
import pythia.plugin
import pythia.util
import pythia.plugins.iita.functions as iita


def build_context(args):
    print("+", end="", flush=True)
    run, ctx, config = args
    context = run.copy()
    context = {**context, **ctx}
    y, x = pythia.util.translate_coords_news(context["lat"], context["lng"])
    context["contextWorkDir"] = os.path.join(context["workDir"], y, x)
    for k, v in run.items():
        if "::" in str(v) and k != "sites":
            fn = v.split("::")[0]
            if fn != "raster":
                res = getattr(pythia.functions, fn)(k, run, context, config)
                if res is not None:
                    context = {**context, **res}
                else:
                    context = None
                    break
    return context


def _generate_context_args(runs, peers, config):
    for idx, run in enumerate(runs):
        for peer in peers[idx]:
            yield run, peer, config


def symlink_wth_soil(output_dir, config, context):
    if "include" in context:
        for include in context["include"]:
            if os.path.exists(include):
                include_file = os.path.join(output_dir, os.path.basename(include))
                if not os.path.exists(include_file):
                    os.symlink(os.path.abspath(include), include_file)
    if "weatherDir" in config:
        weather_file = os.path.join(output_dir, "{}.WTH".format(context["wsta"]))
        if not os.path.exists(weather_file):
            os.symlink(
                os.path.abspath(os.path.join(config["weatherDir"], context["wthFile"])),
                os.path.join(weather_file),
            )
    for soil in context["soilFiles"]:
        soil_file = os.path.join(output_dir, os.path.basename(soil))
        if not os.path.exists(soil_file):
            os.symlink(os.path.abspath(soil), os.path.join(output_dir, os.path.basename(soil)))
def split_levels(levels, max_size):
    for i in range(0, len(levels), max_size):
        yield levels[i:i + max_size]


def iita_build_treatments(context):
    pdates = iita.pdate_factors(context["startYear"], 3, 7, 52)
    hdates = iita.hdate_factors(pdates, 293, 7, 23)

    # date offset hack
    min_date = date(1984, 1, 1)
    sdates = [
        pythia.util.to_julian_date(
            pythia.functions._bounded_offset(pythia.util.from_julian_date(d),
                                             timedelta(days=-30),
                                             min_val=min_date)) for d in pdates
    ]
    context["pdates"] = pdates
    context["hdates"] = hdates
    context["sdates"] = sdates
    context["factors"] = [{
        "tname":
        "iita_p{}_h{}".format(pdates[pf - 1], hdates[hf - 1]),
        "mp":
        pf,
        "mh":
        hf,
    } for pf, hf in iita.generate_factor_list(52, 23)]
    return context


def compose_peerless(context, config, env):
    print(".", end="", flush=True)
    this_output_dir = context["contextWorkDir"]
    context = iita_build_treatments(context)
    #context["contextWorkDir"] = this_output_dir
    
    batch_chunks = config["dssat"].get("batch_chunks", 99)
    symlink_wth_soil(this_output_dir, config, context)
    
    for out_suffix, split in enumerate(split_levels(context["factors"], batch_chunks)):
        
        context["treatments"] = split
        xfile = pythia.template.render_template(env, context["template"],
                                                    context)
        #xfile_name = "NGSP00{:>02d}.CSX".format(out_suffix)
        template_name = context["template"]
        if out_suffix < 10:
            xfile_name = template_name[0:7] + str(out_suffix) + template_name[-4:]
        else:
            xfile_name = template_name[0:6] + str(out_suffix) + template_name[-4:]
        with open(os.path.join(this_output_dir, xfile_name), "w") as f:
            f.write(xfile)
   # Write the batch file
    with open(
            os.path.join(
                this_output_dir,
                config["dssat"].get("batchFile", "DSSBATCH.v47")),
            "w",
    ) as f:
        f.write("$BATCH(PYTHIA)\n")
        f.write(
            "@FILEX                                                  "
            "                                      TRTNO     RP     "
            "SQ     OP     CO\n")
        for out_suffix, treatments in enumerate(
                split_levels(context["factors"], batch_chunks)):
            for trtno in range(len(treatments)):
                #filename = "NGSP00{:>02d}.CSX".format(out_suffix)
                template_name = context["template"]
                if out_suffix < 10:
                    filename = template_name[0:7] + str(out_suffix) + template_name[-4:]
                else:
                    filename = template_name[0:6] + str(out_suffix) + template_name[-4:]
                f.write("{:<94s}{:>5d}      1      0      0      0\n".
                        format(filename, trtno + 1))
    return context["contextWorkDir"]


def execute(config, plugins):
    runs = config.get("runs", [])
    if len(runs) == 0:
        return
    runlist = []
    for run in runs:
        pythia.io.make_run_directory(os.path.join(config["workDir"], run["name"]))
    peers = [pythia.io.peer(r, config.get("sample", None)) for r in runs]
    pool_size = config.get("threads", mp.cpu_count() * 10)
    print("RUNNING WITH POOL SIZE: {}".format(pool_size))
    env = pythia.template.init_engine(config["templateDir"])
    with mp.pool.ThreadPool(pool_size) as pool:
        for context in pool.imap_unordered(
            build_context, _generate_context_args(runs, peers, config), 250
        ):
            if context is not None:
                pythia.io.make_run_directory(context["contextWorkDir"])
                # Post context hook
                logging.debug("[PEERLESS] Running post_build_context plugins")
                context = pythia.plugin.run_plugin_functions(
                    pythia.plugin.PluginHook.post_build_context, plugins, context=context
                )
                runlist.append(os.path.abspath(compose_peerless(context, config, env)))
            else:
                print("X", end="", flush=True)
    if config["exportRunlist"]:
        with open(os.path.join(config["workDir"], "run_list.txt"), "w") as f:
            [f.write(f"{x}\n") for x in runlist]
    print()
