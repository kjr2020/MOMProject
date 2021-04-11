f = open('sleeptask', 'r')

while True:
    line=f.readline()
    if line=='':
         break
    print(line.rstrip('\n'))

f.close()