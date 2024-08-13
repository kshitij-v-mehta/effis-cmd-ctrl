import adios2
from mpi4py import MPI
import os, sys, traceback
import time, datetime
import effis.api as effis
from utils.logger import logger


def cleanup(writer, checkpoint_engine, stream2_writer):
    writer.Close()
    checkpoint_engine.Close()

    if stream2_writer is not None:
        stream2_writer['writer'].Close()
        logger.debug("Simulation closed stream2 engine")


def checkpoint(adios2_writer, v, i, t):
    if MPI.COMM_WORLD.Get_rank()==0: logger.info("simulation main app performing checkpoint")
    adios2_writer.BeginStep()
    adios2_writer.Put(v, f"Checkpoint {i}, timestep {t}")
    adios2_writer.EndStep()


def init_adios_objects(ad2):
    io  = ad2.DeclareIO("IO")
    io.SetEngine("BP5")
    
    io_c = ad2.DeclareIO("Checkpoint")
    io_c.SetEngine("BP5")
    
    v1 = io.DefineVariable("Str")
    v2 = io_c.DefineVariable("Str")
    
    writer            = io.Open   ("test.bp",       adios2.Mode.Write, MPI.COMM_WORLD)
    checkpoint_engine = io_c.Open ("checkpoint.bp", adios2.Mode.Write, MPI.COMM_WORLD)

    return writer, checkpoint_engine, v1, v2


def write_stream2(stream2_writer, ad2):
    """
    Write a step of stream2.bp
    """
    if stream2_writer is None:
        io = ad2.DeclareIO("Stream2")
        io.SetEngine("BP5")

        v1 = io.DefineVariable("Stream2_var1")
        writer = io.Open("stream2.bp", adios2.Mode.Write, MPI.COMM_WORLD)

        stream2_writer = dict()
        stream2_writer["io"] = io
        stream2_writer["var"] = v1
        stream2_writer["writer"] = writer

    stream2_writer['writer'].BeginStep()
    stream2_writer['writer'].Put(stream2_writer["var"], f"{datetime.datetime.today()}")
    stream2_writer['writer'].EndStep()

    return stream2_writer


def disable_stream2(flag_dict):
    """A callback for effis to disable writing stream2.bp"""
    flag_dict['flag'] = True
    if MPI.COMM_WORLD.Get_rank() == 0: logger.info("Callback; Disabled stream2 in the simulation")


def enable_stream2(flag_dict):
    """A callback for effis to enable writing stream2.bp"""
    flag_dict['flag'] = True
    if MPI.COMM_WORLD.Get_rank() == 0: logger.info("Callback; Enabled stream2 in the simulation")


def perform_computation():
    time.sleep(1)


def main():
    try:
        app_name = f"{os.path.basename(sys.argv[0])}"
        rank = MPI.COMM_WORLD.Get_rank()

        effis_overhead = 0.0
        total_time = 0.0
        nt = 20
        icheckpoint = 0

        stream2_enabled = {'flag': False}

        program_timer = time.time()
        ad2 = adios2.ADIOS()

        writer, checkpoint_engine, v1, v2 = init_adios_objects(ad2)
        stream2_writer = None

        if rank == 0:
            logger.info(f"{app_name} initialized adios. Now calling effis.init")
        
        if rank==0: timer_start = time.time()
        effis.init(os.path.basename(sys.argv[0]), checkpoint, None, heartbeat_monitoring=False, heart_rate=2.0)
        if rank==0: effis_overhead += time.time()-timer_start

        # Begin timestepping
        if rank==0: logger.info(f"{app_name} starting timestepping")
        for t in range(nt):
            if rank == 0:
                logger.info(f"{app_name} starting timestep {t}")

            # Send a heartbeat to EFFIS
            if rank==0:
                effis.heartbeat()

            # Science app performs some computations
            perform_computation()

            # Write a step of test.bp
            writer.BeginStep()
            writer.Put(v1, f"Timestep {t}/{nt-1} from {app_name}, P{rank}")
            writer.EndStep()

            # This is dynamically controlled. stream2_enabled is set/unset by effis.
            # If enabled, write a step of stream2.bp
            if stream2_enabled['flag']:
                if rank == 0: logger.info(f"{app_name} stream2 enabled. Writing a step")
                stream2_writer = write_stream2(stream2_writer, ad2)

            # Write a checkpoint every few timesteps
            if t % 3 == 0 and t != 0:
                icheckpoint += 1
                checkpoint(checkpoint_engine, v2, icheckpoint, t)

            logger.debug(f"{app_name} calling effis_check")
            if rank==0: timer_start = time.time()

            # Call effis_check to check for signals.
            # Pass args if applicable for the checkpoint and cleanup routines
            # Pass a callback for other signals that the simulation wants to subscribe to
            signals_subscribed = \
                {"START_STREAM2": {'cb': enable_stream2,  'args': stream2_enabled},
                 "STOP_STREAM2":  {'cb': disable_stream2, 'args': stream2_enabled}, }

            if 1 == effis.check(checkpoint_args = (checkpoint_engine, v2, icheckpoint, t),
                                cleanup_args = (writer, checkpoint_engine),
                                signal_callbacks = signals_subscribed):

                if rank==0:
                    logger.warning(f"{app_name} app received 1 from effis.check. Terminating loop")
                    effis_overhead = time.time() - timer_start
                break
            if rank==0: effis_overhead += time.time() - timer_start

        # Timestepping loop is done. Cleanup and exit
        if rank==0: logger.info(f"{app_name} calling cleanup")
        cleanup(writer, checkpoint_engine, stream2_writer)

        # Call effis_finalize to terminate effis client and server threads
        if rank==0: logger.info(f"{app_name} calling effis_finalize")
        if rank==0: timer_start = time.time()
        effis.finalize()
        if rank==0: effis_overhead += time.time() - timer_start
        
        if rank==0:
            logger.info(f"{app_name} effis overhead: {round(effis_overhead, 3)} seconds")
            logger.info(f"{app_name} total program time: {round(time.time()-program_timer, 2)} seconds")
        logger.info(f"Rank {rank} of {app_name} done. Exiting.")

    except Exception as e:
        print(e)
        print(traceback.format_exc())


if __name__ == '__main__':
    main()

