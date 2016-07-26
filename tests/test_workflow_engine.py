"""
Example unit tests for workflow_engine package
"""
import unittest
import desc.workflow_engine

class workflow_engineTestCase(unittest.TestCase):
    def setUp(self):
        self.message = 'Hello, world'
        
    def tearDown(self):
        pass

    def test_run(self):
        foo = desc.workflow_engine.workflow_engine(self.message)
        self.assertEquals(foo.run(), self.message)

    def test_failure(self):
        self.assertRaises(TypeError, desc.workflow_engine.workflow_engine)
        foo = desc.workflow_engine.workflow_engine(self.message)
        self.assertRaises(RuntimeError, foo.run, True)

if __name__ == '__main__':
    unittest.main()
