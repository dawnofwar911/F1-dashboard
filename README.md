# F1-dashboard

This application uses a dash server to display data from an F1 race. It either connects to a live event 5 mins before the session starts or can be connected manually. It also is able to replay any old recordings using the same data processing.

### Running the application

To run the application pull the git repo and install everything in the requirements.txt, the submodule is the other custom python module required, but the requirements.txt does resolve to that location.

Once cloned, run the application with:

`python main.py`

This will launch a dash dev server on `0.0.0.0:8050`, configurable in the config.py file

### Other Branches

Other branches contain other ways of deploying the app for now.

#### ha-addon

ha-addon is a branch that can be installed as a home assistant addon

Just clone the repo in your addon folder in home assistant, refresh the addons and you can install this addon.

To allow the server to access your home assistant instance remotely, you will need the ingress HACS addon. once installed the code needed in configuration.yaml is:

```
ingress:
  f1_panel_custom:
    title: "F1 Dashboard"
    icon: mdi:go-kart-track
    work_mode: ingress
    url: http://homeassistant.local:8050
```
#### Waitress

waitress is a branch that offers a way to run the app with a production server. The ha-addon does this, but this is just another branch with that implementation but not laid out the same way the ha-addon is.

To run this with waitress use the command:

`waitress-serve --host 0.0.0.0 --port 8050 main:server`

It can also still be run with the development server using:

`python main.py`

