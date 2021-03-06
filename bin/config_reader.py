#!/usr/bin/env python3
# coding: utf-8
# packages
import os
import re 

used_properties_keys=['log_path','logfilename_path','binfolder_path','requirements']
def get_config():
    """ Function to read the Config_properties file and have them in form key_value pair in dictionary config_dict """
    try:
        config_dict=dict()     
        ## properties file name cannot be parameterized
        with open('../cfg/conf.txt', "rt") as f:
            for line in f:
                if not line.startswith('#'):
                    l = line.strip()
                    key_value = l.split('=')
                    key = key_value[0].strip()
                    config_dict[key] = ' '.join(key_value[1:]).strip(' "')
            config_dict = {k:(int(v) if v.isnumeric() else v ) for k,v in config_dict.items() if k in used_properties_keys}
            f.close()
            return config_dict
    except Exception as e:
        print('Exception in readConfig as:: ', e)


if __name__ == '__main__':
    config = get_config() 
    print('config==', config)
