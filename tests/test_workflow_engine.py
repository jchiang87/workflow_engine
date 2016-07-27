"""
Unit tests for workflow engine xml generator code.
"""
from xml.dom import minidom
import unittest
import desc.workflow_engine.workflow_engine as engine

pipeline = engine.Pipeline('WLPipeline')
main_task = pipeline.main_task
#main_task.set_variables(variables)

catsel = main_task.create_process('catalogSelection', engine.job())
catsel_nt = main_task.create_parallel_process('catalogSelectionNullTest',
                                              job=engine.job(),
                                              requirements=[catsel])

pz = main_task.create_parallel_process('photoZCharacterization',
                                       job=engine.job(),
                                       requirements=[catsel_nt])
pz_nt = main_task.create_parallel_process('photoZCharNullTest',
                                          job=engine.job(),
                                          requirements=[pz])

tomo_bin = main_task.create_process('tomographicBinning', job=engine.job(),
                                    requirements=[pz_nt])
tomo_bin_nt = main_task.create_parallel_process('tomoBinningNullTest',
                                                job=engine.job(),
                                                requirements=[tomo_bin])

dndz_inf = main_task.create_process('dNdzInference', job=engine.job(),
                                    requirements=[tomo_bin_nt, pz_nt])
dndz_inf_nt = main_task.create_parallel_process('dNdzInferenceNullTest',
                                                job=engine.job(),
                                                requirements=[dndz_inf])

tpcf = main_task.create_process('2PCFEstimate', job=engine.job(),
                                requirements=[tomo_bin_nt])
tpcf_nt = main_task.create_parallel_process('2PCFEstimateNullTest',
                                            job=engine.job(),
                                            requirements=[tpcf])

cov_model = main_task.create_process('covarianceModel', job=engine.job(),
                                     requirements=[tpcf_nt])
cov_model_nt = main_task.create_parallel_process('covarianceModelNullTest',
                                                 job=engine.job(),
                                                 requirements=[cov_model])
tjp_cosmo = main_task.create_process('TJPCosmo',job=engine.job(),
                                     requirements=[cov_model_nt, tpcf_nt,
                                                   dndz_inf_nt])

#class WorkflowEngineTestCase(unittest.TestCase):
#    def setUp(self):
#        pass
#
#    def tearDown(self):
#        pass
#
#    def test_SubTaskInterface(self):
#        subtask = self.generator.taskFactory(task_name)
#        process = self.generator.processFactory(process_name)
#        subtask.add_process(process)
#
#    def test_failure(self):
#        self.assertRaises(TypeError, desc.workflow_engine.workflow_engine)
#        foo = desc.workflow_engine.workflow_engine(self.message)
#        self.assertRaises(RuntimeError, foo.run, True)
#
#if __name__ == '__main__':
#    unittest.main()
