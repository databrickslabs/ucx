import sys

name_1 = "whatever"
name_2 = f"{name_1}"
sys.path.append(f"{name_2}")
names = [f"{name_2}", name_2]
for name in names:
    sys.path.append(name)
