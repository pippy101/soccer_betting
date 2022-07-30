"""
writes messages to txt file
definitely not *good*, but it works
"""

from datetime import datetime
import time
import traceback

class Log:
    def __init__(self, path):
        self.path = path
        self.writing = False
    
    def write(self, content):
        while self.writing:
            time.sleep(0.1)

        self.writing = True
        with open(f'{self.path}', "a") as fp:
            fp.write(f"---- {datetime.now().strftime('%H:%M:%S %d/%m/%Y')} ----------------------\n")
            if type(content) == str:
                fp.write(content)
            # assumes message is an error, if not a string
            else:
                fp.write(''.join(traceback.format_exception(content))+"\n")
        self.writing = False

log = Log(r"log.txt")
