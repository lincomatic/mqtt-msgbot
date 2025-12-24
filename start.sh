source bin/activate
nohup python mqtt_msgbot.py -c config-socal.ini </dev/null > socal.txt 2>&1 &
nohup python mqtt_msgbot.py -c config-sfbay.ini </dev/null > sfbay.txt 2>&1 &
nohup python mqtt_msgbot.py -c config-sac.ini </dev/null > sac.txt 2>&1 &
ps ax | grep msgbot
