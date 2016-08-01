"""
Script to generate xml for running phosim jobs with the SLAC workflow engine.
"""
from __future__ import absolute_import, print_function
import os
import desc.workflow_engine.workflow_engine as engine

pipeline = engine.Pipeline('JC_phoSim_pipeline', '0.1')

main_task = pipeline.main_task
main_task.notation = 'PhoSim Execution Pipeline'

main_task.set_variables()

# Reset output and script directories at SLAC and NERSC.
slac_root_dir = '/nfs/farm/g/lsst/u/jchiang/workflow_engine_tests/phosim_pipeline'
slac_path = lambda x: os.path.join(slac_root_dir, x)

nersc_root_dir = '/global/cscratch1/sd/jchiang8/workflow_engine_tests/phosim_pipeline'
nersc_path = lambda x: os.path.join(nersc_root_dir, x)

main_task.set_variable('SLAC_OUTPUT_DATA_DIR', slac_path('output'))
main_task.set_variable('NERSC_OUTPUT_DATA_DIR', nersc_path('output'))

main_task.set_variable('SLAC_SCRIPT_LOCATION', slac_path('scripts'))
main_task.set_variable('NERSC_SCRIPT_LOCATION', nersc_path('scripts'))

main_task.set_variable('SCRIPT_NAME', 'phosim_pipeline_workflow.py')

setupVisits = main_task.create_process('setupVisits')

setupPhosim = main_task.create_process('setupPhosim', job_type='script',
                                       requirements=[setupVisits])

singleVisitTask = engine.Task('singleVisitTask')
smokeTest = singleVisitTask.create_process('smokeTest')
runPhoSim = singleVisitTask.create_process('runPhoSim',
                                           requirements=[smokeTest])
phoSimReg = singleVisitTask.create_process('phoSimReg',
                                           requirements=[runPhoSim])
phoSimFinalize = singleVisitTask.create_process('phoSimFinalize',
                                                job_type='script',
                                                requirements=[phoSimReg])

setupPhosim.add_subtask(singleVisitTask)

wrapUp = main_task.create_process('wrapUp', job_type='script',
                                  requirements=[phoSimFinalize])

with open('phosim_pipeline.xml', 'w') as output:
    output.write(pipeline.toxml() + '\n')

pipeline.write_python_module(clobber=True)
pipeline.write_process_scripts()

