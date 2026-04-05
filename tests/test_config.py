from colorama import Fore, Style
FORM_ID = "eab639f7-78c4-4e08-bd27-756bac5cf571" #1ФК
# FORM_ID = "3b5ca99e-cdc7-4590-b4d7-b9d6d95ebc69" #5ФК
# FORM_ID = "ead5f85c-a1d4-4c38-abde-789699ce3586" #9ФК


MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "sport_data"

OK = Fore.GREEN + "[OK]" + Style.RESET_ALL
ERR = Fore.RED + "[ERR]" + Style.RESET_ALL
INF = Fore.CYAN + "[INFO]" + Style.RESET_ALL
WARN = Fore.YELLOW + "[WARN]" + Style.RESET_ALL