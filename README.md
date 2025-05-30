# F1-dashboard

This application uses a dash server to display data from an F1 race. It either connects to a live event 5 mins before the session starts or can be connected manually. It also is able to replay any old recordings using the same data processing.

### Running the application

To run the application pull the git repo and install everything in the requirements.txt, the submodule is the other custom python module required, but the requirements.txt does resolve to that location.

Once cloned, to run this with waitress navigate to app/ and use the command:

`waitress-serve --host 0.0.0.0 --port 8050 main:server`

It can also still be run with the development server using:

`python main.py`

This will launch a dash dev server on `0.0.0.0:8050`, configurable in the config.py file
