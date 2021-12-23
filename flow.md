1. User enters the sensor menu
2. Click add sensor
3. Entering info
     - sensor name (sensor identifier)
     - protected subnet
     - external subnet
     - oinkcode
4. Response
     - sensor_key
     - device_id
     - installer link
5. User opens the installer
6. Installer asks for input
     - username
     - passwords
     - sensor_key
     - device_id
7. Installer checks device_id, then looks at sensor_key
8. If it is correct, download the required file immediately
     - kaspa-snoqtt.tar.gz
         - dockerfile/
         - service-topic.py
         - snoqtt.conf
         - build_snoqtt.sh
         - rmi_snoqtt.sh
         - start_snoqtt.sh
         - stop_snoqtt.sh