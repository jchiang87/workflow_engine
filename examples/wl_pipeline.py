"""
Script to generate xml for weak lensing tomographic binning pipeline.
See https://confluence.slac.stanford.edu/display/LSSTDESC/WL+Tomography+Pipeline+Hack
"""
from __future__ import absolute_import, print_function
import os
import desc.workflow_engine.workflow_engine as engine

pipeline = engine.Pipeline('JC_WLPipeline', '0.3')
main_task = pipeline.main_task
main_task.notation = 'Weak Lensing Pipeline for Cosmological Parameter Estimation'

# Set the main task variables from the data/main_task_variables.txt
# configuration file.
main_task.set_variables()

# Reset output and script directories at SLAC and NERSC.
slac_root_dir = '/nfs/farm/g/lsst/u/jchiang/workflow_engine_tests/wl_pipeline'
slac_path = lambda x: os.path.join(slac_root_dir, x)

nersc_root_dir = '/global/cscratch1/sd/jchiang8/workflow_engine_tests/wl_pipeline'
nersc_path = lambda x: os.path.join(nersc_root_dir, x)

main_task.set_variable('SLAC_OUTPUT_DATA_DIR', slac_path('output'))
main_task.set_variable('NERSC_OUTPUT_DATA_DIR', nersc_path('output'))

main_task.set_variable('SLAC_SCRIPT_LOCATION', slac_path('scripts'))
main_task.set_variable('NERSC_SCRIPT_LOCATION', nersc_path('scripts'))

# Catalog selection process (and null tests).
catsel = main_task.create_process('catalogSelection')
catsel.notation = 'Make selections on the DM Catalog data'
catsel_nt = main_task.create_parallel_process('catSelectionNullTest',
                                              requirements=[catsel])

# Photo-z characterization.
pz = main_task.create_parallel_process('photoZCharacterization',
                                       requirements=[catsel_nt])
pz.notation = 'Photo-z characterization task'
pz_nt = main_task.create_parallel_process('photoZCharNullTest',
                                          requirements=[pz])

# Tomographic binning of catalog data.
tomo_bin = main_task.create_process('tomographicBinning',
                                    requirements=[pz_nt])
tomo_bin.notation = 'Tomographic binning of catalog selections'
tomo_bin_nt = main_task.create_parallel_process('tomoBinningNullTest',
                                                requirements=[tomo_bin])

# N(z) inference from tomographic binning and P(z).
dndz_inf = main_task.create_process('dNdzInference',
                                    requirements=[tomo_bin_nt, pz_nt])
dndz_inf.notation = 'Inference of N(z)'
dndz_inf_nt = main_task.create_parallel_process('dNdzInferenceNullTest',
                                                requirements=[dndz_inf])

# Two-point correlation function estimation.
tpcf = main_task.create_process('TwoPCFEstimate',
                                requirements=[tomo_bin_nt])
tpcf.notation = '2-point correlation function estimation'
tpcf_nt = main_task.create_parallel_process('TwoPCFEstimateNullTest',
                                            requirements=[tpcf])

# Covariance matrix calculation.
cov_model = main_task.create_process('covarianceModel',
                                     requirements=[tpcf_nt])
cov_model.notation = 'Covariance matrix calculation'
cov_model_nt = main_task.create_parallel_process('covarianceModelNullTest',
                                                 requirements=[cov_model])

# Joint probes cosmological infererence.
tjp_cosmo = main_task.create_process('TJPCosmo',
                                     requirements=[cov_model_nt, tpcf_nt,
                                                   dndz_inf_nt])
tjp_cosmo.notation = 'Joint probe cosomlogical parameter estimation'

# Write the xml to an output file.
with open('wl_pipeline.xml', 'w') as output:
    output.write(pipeline.toxml() + '\n')

# Create the python module with the substream launching functions for
# the parallelized subtasks.  The module name is determined from the
# main task variables.
var = pipeline.write_python_module()

# Create stubs for the bash scripts expected by the SLAC workflow engine.
pipeline.write_process_scripts()
