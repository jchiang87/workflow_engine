"""
Unit tests for workflow engine xml generator code.
"""
from __future__ import print_function, absolute_import
from builtins import str
import os
import unittest
from xml.dom import minidom
import desc.workflow_engine.workflow_engine as engine

class WorkflowEngineTestCase(unittest.TestCase):
    def setUp(self):
        self.main_task_name = 'my_pipeline'
        self.version = '1.0'
        self.pipeline = engine.Pipeline(self.main_task_name, self.version)
        varfile = os.path.join(os.environ['WORKFLOW_ENGINE_DIR'],
                               'tests', 'main_task_test_variables.txt')
        self.pipeline.main_task.set_variables(varfile=varfile)
        self.process_name = 'my_process'
        self.parallel_process_name = 'my_parallel_process'

    def tearDown(self):
        for filename in (self.pipeline.get_module_name(),
                         self.process_name,
                         self.parallel_process_name):
            try:
                os.remove(filename)
            except OSError:
                pass
        del self.main_task_name
        del self.version
        del self.pipeline

    def test_pipeline_creation(self):
        doc = minidom.parseString(str(self.pipeline))
        tasks = doc.getElementsByTagName('task')
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0].getAttribute('name'), self.main_task_name)
        self.assertEqual(tasks[0].getAttribute('version'), self.version)

    def test_process_creation(self):
        main_task = self.pipeline.main_task
        std_job_name = 'my_std_job'
        long_job_name = 'my_long_job'
        script_name = 'my_script'

        std_job = main_task.create_process(std_job_name)
        doc = minidom.parseString(str(std_job))
        process = doc.getElementsByTagName('process')[0]
        self.assertEqual(process.getAttribute('name'), std_job_name)

        jobs = doc.getElementsByTagName('job')
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0].getAttribute('maxCPU'), '${MAXCPU}')
        scripts = doc.getElementsByTagName('script')
        self.assertEqual(len(scripts), 0)

        long_job = main_task.create_process(long_job_name, job_type='long',
                                            requirements=[std_job])
        doc = minidom.parseString(str(long_job))
        jobs = doc.getElementsByTagName('job')
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0].getAttribute('maxCPU'), '${MAXCPULONG}')
        scripts = doc.getElementsByTagName('script')
        self.assertEqual(len(scripts), 0)
        self.assertEqual(std_job, long_job.requirements[0])

        script = main_task.create_process(script_name, job_type='script',
                                          requirements=[std_job, long_job])
        doc = minidom.parseString(str(script))
        jobs = doc.getElementsByTagName('job')
        self.assertEqual(len(jobs), 0)
        scripts = doc.getElementsByTagName('script')
        self.assertEqual(len(scripts), 1)
        self.assertIn(long_job, script.requirements)
        self.assertIn(std_job, script.requirements)
        depends = doc.getElementsByTagName('depends')
        self.assertEqual(len(depends), 1)
        afters = depends[0].getElementsByTagName('after')
        self.assertEqual(afters[0].getAttribute('process'), std_job_name)
        self.assertEqual(afters[1].getAttribute('process'), long_job_name)

        # Test for invalid process name.
        self.assertRaises(RuntimeError, main_task.create_process, *('2PCF',))

    def test_parallel_process_creation(self):
        main_task = self.pipeline.main_task
        process_name = self.process_name
        process = main_task.create_process(process_name)
        parallel_process_name = self.parallel_process_name
        parallel_process \
            = main_task.create_parallel_process(parallel_process_name)
        doc = minidom.parseString(str(self.pipeline))
        processes = doc.getElementsByTagName('process')
        self.assertEqual(len(processes), 3)
        self.assertEqual(processes[1].getAttribute('name'),
                         'setup_%ss' % parallel_process_name)
        self.assertEqual(processes[2].getAttribute('name'),
                         parallel_process_name)
        scripts = processes[1].getElementsByTagName('script')
        self.assertEqual(len(scripts), 1)
        jobs = processes[2].getElementsByTagName('job')
        self.assertEqual(len(jobs), 1)

        # Test for failure.
        self.assertRaises(RuntimeError, main_task.create_parallel_process,
                          *(parallel_process_name, 'script'))

    def test_python_module_creation(self):
        main_task = self.pipeline.main_task
        process_name = self.process_name
        module_name = self.pipeline.get_module_name()
        main_task.create_parallel_process(process_name)
        self.pipeline.write_python_module(clobber=True)
        with open(module_name) as file_obj:
            code = compile(file_obj.read(), module_name, 'exec')
            exec(code)
        for process in main_task.processes:
            if process.subtasks:
                subtask_name = process.subtasks[0].name
                self.assertIsInstance(eval(process.name), type(lambda : 1))
                self.assertIsInstance(eval(subtask_name + '_jobs'), list)
                exec(process.name)

        # Check that an existing module file doesn't get overwritten.
        # Create an empty file for comparison.
        with open(module_name, 'w') as output:
            pass
        self.pipeline.write_python_module()
        with open(module_name, 'r') as file_obj:
            self.assertEqual(len(file_obj.read()), 0)

    def test_task_variable_interface(self):
        varname = 'SITE'
        value = 'NERSC'
        main_task = self.pipeline.main_task

        self.assertEqual(main_task.get_variable(varname), value)
        self.assertRaises(RuntimeError, main_task.get_variable, 'foobar')

        new_value = 'SLAC'
        main_task.set_variable(varname, new_value)
        self.assertEqual(main_task.get_variable(varname), new_value)
        self.assertRaises(RuntimeError, main_task.set_variable,
                          *('foobar', new_value))

    def test_script_generation(self):
        main_task = self.pipeline.main_task
        process_name = self.process_name
        process = main_task.create_process(process_name)
        parallel_process_name = self.parallel_process_name
        parallel_process \
            = main_task.create_parallel_process(parallel_process_name)
        # Write one of the scripts by hand, and check that the
        # Pipeline.write_process_scripts method does not overwrite it.
        script_content = '''echo "Running %s."
User-defined script content.
''' % process_name
        with open(process_name, 'w') as file_obj:
            file_obj.write(script_content)
        num_scripts = self.pipeline.write_process_scripts()
        self.assertEqual(num_scripts, 2)
        with open(parallel_process_name, 'r') as file_obj:
            self.assertEqual(len(file_obj.readlines()), 1)
        with open(process_name, 'r') as file_obj:
            self.assertEqual(file_obj.read(), script_content)

    def test_multiprocess_subtask(self):
        main_task = self.pipeline.main_task
        main_task.set_variable('SCRIPT_NAME', 'multi_process_subtask.py')
        outer_process = main_task.create_process('setup_process',
                                                 job_type='script')
        subtask = engine.Task('my_subtask')
        sub_process1 = subtask.create_process('subprocess1')
        sub_process2 = subtask.create_process('subprocess2', job_type='script',
                                              requirements=[sub_process1])
        sub_process3 = subtask.create_process('subprocess3',
                                              requirements=[sub_process2])
        outer_process.add_subtask(subtask)
        module_name = self.pipeline.get_module_name()
        self.pipeline.write_python_module(clobber=True)
        doc = minidom.parseString(self.pipeline.toxml())
        tasks = doc.getElementsByTagName('task')
        processes = tasks[1].getElementsByTagName('process')
        self.assertEqual(sub_process1.name, processes[0].getAttribute('name'))
        self.assertEqual(sub_process2.name, processes[1].getAttribute('name'))
        self.assertEqual(sub_process3.name, processes[2].getAttribute('name'))
        with open(module_name) as file_obj:
            code = compile(file_obj.read(), module_name, 'exec')
            exec(code)
        self.assertIsInstance(eval(subtask.name + '_jobs'), type([]))
        self.assertIsInstance(eval(sub_process2.name), type(lambda : 1))
        self.assertIsInstance(eval(outer_process.name), type(lambda : 1))

if __name__ == '__main__':
    unittest.main()
