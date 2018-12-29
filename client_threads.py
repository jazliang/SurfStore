import os
import threading


def delete():
    os.system('python3 client.py config.txt delete 2.png')


def upload():
    os.system('python3 client.py config.txt upload 2.png')



target = [delete, upload, delete]

threads = [threading.Thread(target=target[i % 3], name='t' + str(i)) for i in range(20)]

for t in threads:
    t.start()