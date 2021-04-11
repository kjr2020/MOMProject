import random

f = open('sleeptask', 'w')

for i in range(3000):
    tmp = str(random.randrange(1,100))
    f.write(tmp.zfill(2)+'\n')

f.close()