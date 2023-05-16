import os
# bashrc
# /export/server/anaconda3/condabin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin

def cli(command):
    res = os.system(command)
    return res

print(cli("ip a"))
# print(cli("echo 你好呀"))