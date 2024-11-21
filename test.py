import sys

print('My python interpreter is located at')
print(sys.executable)

print('It looks for modules in these directories')
for p in sys.path:
    print(p)

try:
    import tkinter
    print('Tkinter imported successfully')
    print('It is located at')
    print(tkinter)
except ImportError:
    print('Tkinter is not installed in this environment')