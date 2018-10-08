import re
import pandas as pd
import numpy as np
import os
class LogParser:
        def __init__(self,logname, indir='./',outdir='./',log_format=None,rex=[]):
            self.path = indir
            self.logName = None

            self.savePath = outdir
            self.logformat = log_format
            self.df_log = None
            self.rex = rex
            self.logname = logname



        def log_to_dataframe(self, log_file, regex, headers, logformat):
            """ Function to transform log file to dataframe
            """
            log_messages = []
            linecount = 0
            with open(log_file, 'r') as fin:
                for line in fin.readlines():
                    #print(line)
                    line = re.sub(r'[^\x00-\x7F]+', '<NASCII>', line)
                    #print(line)
                    try:
                        match = regex.search(line.strip())
                        message = [match.group(header) for header in headers]
                        #print(message)
                        log_messages.append(message)
                        linecount += 1
                    except Exception as e:
                        pass
            logdf = pd.DataFrame(log_messages, columns=headers)
            logdf.insert(0, 'LineId', None)
            logdf['LineId'] = [i + 1 for i in range(linecount)]
            return logdf


        def generate_logformat_regex(self, logformat):
                """ Function to generate regular expression to split log messages
                """
                headers = []
                #print(logformat)
                splitters = re.split(r'(<[^<>]+>)', logformat)
                #print(splitters)
                regex = ''
                #print(len(splitters))
                #print(splitters)
                for k in range(len(splitters)):
                    #print(k)
                    if k % 2 == 0:
                        splitter = re.sub(' +', '\s+', splitters[k])
                        #print(splitter)
                        regex += splitter
                        #print(regex)
                    else:
                        header = splitters[k].strip('<').strip('>')
                        #print(header)
                        regex += '(?P<%s>.*?)' % header
                        headers.append(header)
                regex = re.compile('^' + regex + '$')
                #print(headers)
                return headers, regex


        def load_data(self):
            headers, regex = self.generate_logformat_regex(self.logformat)
            self.df_log = self.log_to_dataframe(os.path.join(self.path, self.logname), regex, headers, self.logformat)
            self.df_log.to_csv(os.path.join(self.savePath, self.logname + '_structured.csv'), index=False)




            return True



