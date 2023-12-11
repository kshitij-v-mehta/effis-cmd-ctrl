import adios2
from mpi4py import MPI
import os
import effis.signals as effis_signals
from effis.api import effis_init, effis_finalize, effis_signal


def main():
    # Only root
    if MPI.COMM_WORLD.Get_rank() != 0:
        return

    app_name = os.path.basename(sys.argv[0])

    # Start effis socket thread
    effis_init(os.path.basename(app_name))

    ad2 = adios2.ADIOS()
    io = ad2.DeclareIO("reader")
    io.SetEngine("BP5")
    engine = io.Open("test.bp", adios2.Mode.Read)
    
    while(engine.BeginStep() == adios2.StepStatus.OK):
        ad_var = io.InquireVariable("Str")
        v = engine.Get(ad_var)

        # Indicate error condition
        if 'timestep 5' in v:
            effis_signal(effis_signals.EFFIS_SIGTERM)

        engine.EndStep()
    engine.Close()

    # Shut the socket thread
    effis_finalize()


if __name__ == '__main__':
    main()
