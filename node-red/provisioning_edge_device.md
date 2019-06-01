These are steps required for setting up an edge device for use with an Edge Analyzer:
- Configure a static IP address on the device for ease of use. Ideally use an address in the 192.168.88.xx range where xx does not conflict with other demo edge devices we are using (see device_list).
- Set the password to 'falkonry'
- Make sure a VNC server is running and that 'falkonry' is the password for connecting
- Make sure Node.js is installed.
- Install node-red (use the Raspberry Pi specific instruction link here. It also works on Tinker Board). https://nodered.org/docs/hardware/raspberrypi. Go into the node red editor, select Manage Pallette and install the node-red-dashboard library.
- Follow the instructions above to configure node-red to run as a service and autostart. See the section Adding Autostart capability using SystemD. You should be able to skip to the step 'sudo systemctl enable nodered.service' and then type 'node-red-start' from the command line.
- Copy the 2 scripts from the Falkonry repo (change_edge_analyzer.sh & restart_edge_analyzer.sh) onto the device. Make them executable '''chmod +x change_edge_analyzer.sh restart_edge_analyzer.sh'''. Move them into /usr/local/bin/. '''sudo mv change_edge_analyzer.sh /usr/local/bin/''' and same for other
- Open the browser on the device adn load the following two flows from the library into node-red
 - ModelOutputFromEdge_flow.json
 - UtilitiesForEdge_flow.json
- Create browser bookmarks for Node-red Editor:localhost:1880 and Node-red UI:localhost:1880/ui
- Edit the autostart file to make the browser launch full screen on startup
'''
sudo nano /home/pi/.config/lxsession/LXDE-pi/autostart or /home/linaro/.config/lxsession/LXDE/autostart (on tinkerboard)
to add the following lines

@xset s noblank
@xset s off
@xset –dpms
@chromium --start-fullscreen http://localhost:1880/ui (it may be chromium-browser on some installs)'''
