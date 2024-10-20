a=input("Enter a string:") 
c={} 
for i in a: 
c[i]=c.get(i,0)+1 
for i in a: 
if c[i]==1: 
print(i) 
break
