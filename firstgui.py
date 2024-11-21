# import tkinter
# print(tkinter.TkVersion)

# pysimpleguikey = eMykJMMYa7W9NKlHbWnBNVlEVPHqlhwUZ1SXIU62IvkGRUlKdFm5V5s2b03mBHlhceiJInsmIbk8xupsYx2QVfuDcn2sV9J4RNC4IF6DMqTJcKzwM6TsgFwqNSTPcl3aMBydwzijTCGnlDjMZKWF5EznZBUrRJlXcGGLxTvseCWT1il6b1nrRaWcZoXaJ1zbamWZ9iu0IljMowxzLjCWJ8OvYqW41mloRsmwl4y9co3BQgiLOpiJJ1kKeWWLxLhHbkiNILsXInkh5dhNbUWuVkMMYfXkNX0HIQjIoXieaPG5lE0wZMSxIEsYIpk6N1vbbZXkBthpbEnTkUicO7inIbisLqChJTD9dZXJNh0jbd2K1Yl4cKkmlzELI6jxoLiqNvDmYy2mN1DlAOi3LMCQJnEZYiXQRbleS6XxNvzFdQWKVDkPIfjEositM0TFEtvGMATKc7vMMnj2AxyvNJCOI8sOI4kjRhh2dNGWVpF3eQHyBopicHmdViz9ICjxoeiKMlTUEdv8MkTec2vKMxjFAyyxNaSTI3stITkfVLtVY1WwlqsBQ4WcRSkPcEmFVNz2cgyeIc6rImmMRY5JbUGYFaujaKGsls0WZWUPBhn7b0WtFRpUbIC65HjSbo2B0PigLvC1JhJVUaEUFckXZzHwJrlacR3DMhiZOiiYIm39MIi74QzzNYSl4uxBNJDmQ7uhMSjzMizxIunb0B=L26773ffa486075db0130261a0ddcf8ecc70703aeec362137fc1d880aa47174d2b5ef582e953a77561c80a5b37ecdfe42850c6c725af73133706dfae1716eaa6dc119a6ac4ef7ef8bdbab91991ebe4d6e07cb4224b37bce83452ce0546c013f1f7823efc712387758f68bde8580cd3bd0f58eb502345f59ff91da7818400dc72f343549b9f8f8769891c581fd793b0faddb9bda2feec6ce6026529da10d15f6f048898ac8fe051d5c327639aa76646804cd3a51eca455ef032fdd46f6bcdbd4400fe76c4af92066647d331855e2f0312c2623661986809ba2aae157b53658da5aab6ab765469f04ea53fc66afbcaea8fdd2bb13aef282b021cfbe86e13c54ab26fa19c32499169175992d734b79a9d0de757402b24f18351adf1e1528dd4b0d523dc38a4f318ddbf9521b62276e867340d92e1ff86e0ae83306ec281c3854cf3df82d5c6d64a2dd6946a13f5aecea7d40b87914991606f156b7a10161e8498ad23ea32a5be87ecaafa11e7c35210bc24dc73c481211020a29a8e8abbfb7638c09cc8e3d90e7b9165d07957f6efbff839970f536ade73686bbee52425c2ed9637c6badd228f73dfc8e6b49c7e0cfca4498bce1a2d2acef54f2a944510655281e104cc1d398b0a14aec1dbe3ea627db386b3320fd7d26fb65e3c9c2d6e946aa7d062b0d752015502d760034cbd0d366d099cbd94a109c43cac76e86b57ff2e3be0a

import PySimpleGUI as sg

# All the stuff inside your window
layout = [
    [sg.Text("What's your name?")],
    [sg.InputText(key='-NAME-')],  # Use key to reference input text
    [sg.Button('Ok'), sg.Button('Cancel')]
]

# Create the Window
window = sg.Window('Hello Example', layout)

# Event Loop to process "events" and get the "values" of the inputs
while True:
    event, values = window.read()

    # if user closes window or clicks cancel
    if event == sg.WIN_CLOSED or event == 'Cancel':
        break

    # Make sure there is a value before greeting
    if values['-NAME-']:
        print('Hello', values['-NAME-'], '!')
    else:
        print('Hello, world!')

window.close()