from __future__ import print_function
from __future__ import absolute_import

import numpy
import glob
import sys, os, gc
import h5py
import logging
import pdb
import cPickle as pickle

from .generate_cloudlets import generate_cloudlets
from .cluster_cloudlets import cluster_cloudlets
from .make_graph import make_graph
from .output_cloud_data import output_cloud_data

try:
	from netCDF4 import Dataset
except:
	try:
		from netCDF3 import Dataset
	except:
		from pupynere import netcdf_file as Dataset

pha_logger=logging.getLogger('pha_debug')


#-------------------

def load_data(filename):
    input_file = Dataset(filename)
        
    core = input_file.variables['core'][:].astype(bool)
    condensed = input_file.variables['condensed'][:].astype(bool)
    plume = input_file.variables['plume'][:].astype(bool)
    u = input_file.variables['u'][:].astype(numpy.double)
    v = input_file.variables['v'][:].astype(numpy.double)
    w = input_file.variables['w'][:].astype(numpy.double)
        
    input_file.close()

    return core, condensed, plume, u, v, w

#---------------

#@profile
def main(MC, save_all=True):
    input_dir = MC['data_directory']
    nx = MC['nx']
    ny = MC['ny']
    nz = MC['nz']
    nt = MC['nt']
    
    cloudlet_items = ['core', 'condensed', 'plume', 'u_condensed', 'v_condensed', \
        'w_condensed', 'u_plume', 'v_plume', 'w_plume']

    filelist = glob.glob('{}/tracking/*nc'.format(MC['data_directory']))
    pha_logger.info('inside main, filelist has {} items'.format(len(filelist)))        
    filelist.sort()

    #if (len(filelist) != nt):
    #    raise Exception("Only %d files found, nt=%d files expected" % (len(filelist), nt))

    if not os.path.exists('output'):
        os.mkdir('output')
    # TEST: Data folder for testing
    if not os.path.exists('hdf5'):
        os.mkdir('hdf5')

    ## # TODO: Parallelize file access (multiprocessing)
    do_cloudlets=True
    
    if do_cloudlets:
        for n, filename in enumerate(filelist):
            print("generate cloudlets; time step: %d" % n)
            core, condensed, plume, u, v, w = load_data(filename)

            cloudlets = generate_cloudlets(core, condensed, plume, u, v, w, MC)

            # NOTE: cloudlet save/load works properly
            # TEST: linear calls instead of for lodop to speed this up?
            with h5py.File('{}/hdf5/cloudlets_{:08g}.h5'.format(MC['output_directory'], n), "w") as f:
                for i in range(len(cloudlets)):
                    grp = f.create_group(str(i))
                    for var in cloudlet_items:
                        if(var in ['core', 'condensed', 'plume']):
                            dset = grp.create_dataset(var, data=cloudlets[i][var][...])
                        else:
                            dset = grp.create_dataset(var, data=cloudlets[i][var])

    ##     gc.collect() # NOTE: Force garbage-collection at the end of loop
    
#----cluster----
    pha_logger.info('inside main')
    print("Making clusters")
    #pdb.set_trace()

    # FIXME: cluster save/load does not work properly
    do_cluster= True
    if do_cluster:
        cluster_cloudlets(MC)

#----graph----

    print("make graph")
    output_dir=MC['output_directory']
    do_graph=True
    if do_graph:
        cloud_graphs, cloud_noise = make_graph(MC)
    else:
        print('skipping make_raph')
        with open('{}/pkl/cloud_graphs.pkl'.format(output_dir),'r') as cg,\
             open('{}/pkl/cloud_noise.pkl'.format(output_dir),'r') as cn:
            cloud_graphs=pickle.load(cg)
            cloud_noise=pickle.load(cn)
        
    
    print("\tFound %d clouds" % len(cloud_graphs))

    # if save_all:
    #     FIXME: Object dtype dtype('object') has no native HDF5 equivalent
    #     with h5py.File('hdf5/graph_data.h5', 'w') as f:
    #         dset = f.create_dataset('cloud_graphs', data=cloud_graphs)
    #         dset = f.create_dataset('cloud_noise', data=cloud_noise)
    #     #cPickle.dump((cloud_graphs, cloud_noise), open('pkl/graph_data.pkl', 'wb'))
            
#----output----

    # TODO: Parallelize file output (multiprocessing)
    for n in range(nt):
        print("output cloud data, time step: %d" % n)

        output_cloud_data(cloud_graphs, cloud_noise, n, MC)
        #n = gc.collect() # Note: Garbage collection
        #print sys.getallocatedblocks(), " blocks allocated"
        # print("Unreacheable objects: ", n)
            
