"""
Unit tests for workflow engine xml generator code.
"""
import unittest
import desc.workflow_engine.workflow_engine as engine

pipeline = engine.Pipeline('WLPipeline')
main_task = pipeline.main_task
main_task.set_variables(variables)

catalog_selection = engine.Process('catalogSelection')
main_task.add_process(catalog_selection)

setup_cat_sel_NTs = engine.Process('setupCatalogSelectionNullTests')
setup_cat_sel_NTs.requires(catalog_selection)

cat_sel_NT_task = engine.Task('catalogSelectionNullTestsTask')
setup_cat_sel_NTs.add_subtask(cat_sel_NT_task)
cat_sel_null_test = engine.Process('catalogSelectionNullTest')
cat_sel_NT_task.add_process(cat_sel_null_test)

class WorkflowEngineTestCase(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_SubTaskInterface(self):
        subtask = self.generator.taskFactory(task_name)
        process = self.generator.processFactory(process_name)
        subtask.add_process(process)

    def test_failure(self):
        self.assertRaises(TypeError, desc.workflow_engine.workflow_engine)
        foo = desc.workflow_engine.workflow_engine(self.message)
        self.assertRaises(RuntimeError, foo.run, True)

if __name__ == '__main__':
    unittest.main()
