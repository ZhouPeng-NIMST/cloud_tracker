from __future__ import print_function
from __future__ import absolute_import

import glob, os, sys
import site
import logging,pdb
logging.basicConfig()

home_dir=os.getenv('HOME')
site.addsitedir('%s/repos' % home_dir)


# Multiprocessing modules
import multiprocessing as mp
from multiprocessing import Pool
PROC = 1

from  cloud_tracker.lib import model_param as mc
from  cloud_tracker.cloudtracker.main import main

# Default working directory for ent_analysis package
cwd = os.getcwd()

# Output profile names
profiles = {'condensed', 'condensed_env', 'condensed_edge', \
    'condensed_shell' , 'core', 'core_env', 'core_edge', 'core_shell', \
    'plume', 'condensed_entrain', 'core_entrain', 'surface'}

def wrapper(module_name, script_name, function_name, filelist):
    pkg = __import__ (module_name, globals(), locals(), ['*'])
    md = getattr(pkg, script_name)
    fn = getattr(md, function_name)
    
    pool = mp.Pool(PROC)
    pool.map(fn, filelist)
    
def run_cloudtracker():
    print("mc here: ",dir(mc))
    try:
        pha_logger=logging.getLogger('pha_debug')
        pha_logger.setLevel(logging.DEBUG)
        # Change the working directory for cloudtracker
        #os.chdir('%s/cloudtracker/' % (cwd))
        pha_logger.info('start logging')
        model_config = mc.model_config

        model_config['tracking_directory'] = mc.data_directory + '/tracking'
        model_config['nt'] = len(glob.glob('{}/*.nc'.format(model_config['tracking_directory'])))
        pha_logger.info('found {} tracking files in {}'.format(model_config['nt'],
                                                               model_config['tracking_directory']))
        # Swap input directory for cloudtracker 
        main(model_config)
    except Exception as ex:
        pha_logger.exception(ex)
        

if __name__ == '__main__':
    run_cloudtracker()
    print('Entrainment analysis completed')
    
