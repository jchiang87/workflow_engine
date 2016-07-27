"""
Unit tests for workflow engine xml generator code.
"""
from __future__ import print_function, absolute_import
import unittest
from xml.dom import minidom
import desc.workflow_engine.workflow_engine as engine

class WorkflowEngineTestCase(unittest.TestCase):
    def setUp(self):
        self.main_task_name = 'my_desc_pipeline'
        self.version = '1.0'
        self.pipeline = engine.Pipeline(self.main_task_name, self.version)

    def tearDown(self):
        del self.main_task_name
        del self.version
        del self.pipeline

    def test_PipelineCreation(self):
        doc = minidom.parseString(str(self.pipeline))
        tasks = doc.getElementsByTagName('task')
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0].getAttribute('name'), self.main_task_name)
        self.assertEqual(tasks[0].getAttribute('version'), self.version)

    def test_ProcessCreation(self):
        main_task = self.pipeline.main_task
        std_job_name = 'my_std_job'
        long_job_name = 'my_long_long'
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
        self.assertTrue(long_job in script.requirements)
        self.assertTrue(std_job in script.requirements)
        depends = doc.getElementsByTagName('depends')
        self.assertEqual(len(depends), 1)
        afters = depends[0].getElementsByTagName('after')
        self.assertTrue(afters[0].getAttribute('process'), std_job_name)
        self.assertTrue(afters[1].getAttribute('process'), long_job_name)

    def test_ParallelProcessCreation(self):
        main_task = self.pipeline.main_task
        process_name = 'my_process'
        process = main_task.create_process(process_name)
        parallel_process_name = 'my_parallel_process'
        parallel_process \
            = main_task.create_parallel_process(parallel_process_name)
        doc = minidom.parseString('<doc>' + str(parallel_process) + '</doc>')
        processes = doc.getElementsByTagName('process')
        self.assertEqual(len(processes), 2)
        self.assertEqual(processes[0].getAttribute('name'),
                         'setup_%ss' % parallel_process_name)
        self.assertEqual(processes[1].getAttribute('name'),
                         parallel_process_name)
        scripts = processes[0].getElementsByTagName('script')
        self.assertEqual(len(scripts), 1)
        jobs = processes[1].getElementsByTagName('job')
        self.assertEqual(len(jobs), 1)

        # Test for failure
        self.assertRaises(ValueError, main_task.create_parallel_process,
                          *(parallel_process_name, 'script'))

if __name__ == '__main__':
    unittest.main()
