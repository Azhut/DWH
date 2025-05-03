from app.features.sheet_parsers.HeaderFixer import fix_header

raw = "Численнос\nть\nобучающих\nся,\nотнесенны\nх по\nсостоянию\nздоровья к\nспецмедгру\nппе\n(человек)"
print(fix_header(raw))


