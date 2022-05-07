import os


id  = "1573972812"
tkn = "5347196612:AAFZVEr7SNfJT06guh12HoYlMUza4QSSfDA"
msg = "Hola IOT"
command = "curl https://api.telegram.org/bot{}/sendMessage?chat_id={}&text={}".format(tkn,id,msg)
print(command)
#os.system(command)