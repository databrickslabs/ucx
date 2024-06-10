import sys
name_1 = "whatever"
sys.path.append(f"{name_1}")
names = [ f"{name_1}", name_1 ]
for name in names:
    sys.path.append(name)
