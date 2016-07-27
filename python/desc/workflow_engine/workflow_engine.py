from collections import OrderedDict
from xml.dom import minidom

__all__ = ['job', 'long_job', 'Pipeline', 'Task', 'Process']

_indent = ' '*4

def check_name(name):
    if len(name) > 30:
        raise ValueError('Process or task name must be 30 characters or fewer')

def job():
    return '<job maxCPU="${MAXCPU}" batchOptions="${BATCH_OPTIONS}" executable="${SCRIPT_LOCATION}/${BATCH_NAME}"/>'

def long_job():
    return '<job maxCPU="${MAXCPULONG}" batchOptions="${BATCH_OPTIONS}" executable="${SCRIPT_LOCATION}/${BATCH_NAME}"/>'

class Pipeline(object):
    def __init__(self, name, version='0.1'):
        self.main_task = Task(name, version=version)

    def __str__(self):
        return str(self.main_task)

class Task(object):
    def __init__(self, name, version=None):
        check_name(name)
        self.name = name
        self.version = version
        self.notation = None
        self.variables = []
        self.processes = OrderedDict()

    def set_variables(self, vars):
        for key, value in vars:
            self.set_variable(key, value)

    def set_variable(self, key, value):
        pass

    def add_process(self, process):
        self.processes[process.name] = process
        if process.supertask is not None:
            raise RuntimeError("Cannot assign a process to more than one task")
        if self.version is None:
            process.supertask = self.name

    def create_process(self, process_name, job=None, requirements=[]):
        process = Process(process_name)
        process.job = job
        self.add_process(process)
        return process

    def create_parallel_process(self, process_name, job, requirements=[]):
        setup_process = Process('setup_' + process_name + 's')
        self.add_process(setup_process)
        for process in requirements:
            setup_process.requires(process)
        subtask = Task(process_name + 'sTask')
        setup_process.add_subtask(subtask)
        inner_process = Process(process_name)
        inner_process.job = job
        subtask.add_process(inner_process)
        return inner_process

    def __str__(self):
        lines = []
        if self.version is not None:
            lines.append('<task name="%s" type="LSST" version="%s">'
                         % (self.name, self.version))
        else:
            lines.append('<task name="%s" type="LSST">' % self.name)
        lines.extend(self.variables)
        lines.extend([str(process) for process in self.processes.values()])
        lines.append('</task>')
        return '\n'.join(lines)

class Process(object):
    def __init__(self, name):
        self.name = name
        self.notation = None
        self.job = None
        self.script = None
        self.requirements = []
        self.subtasks = []
        self.supertask = None

    def requires(self, process):
        self.requirements.append(process)

    def add_subtask(self, task):
        self.subtasks.append(task)

    def __str__(self):
        lines = []
        lines.append('<process name="%s" site="${JOBSITE}">' % self.name)
        if self.notation is not None:
            lines.append('%s<notation>%s</notation>' % (_indent, self.notation))
        # A process comprises only either a job or a script.
        if self.job is not None:
            lines.append(_indent + self.job)
        else:
            lines.append(_indent + '<script><![CDATA[')
            lines.append('  execfile("%s/%s" % (SLAC_SCRIPT_LOCATION, SCRIPT_NAME))')
            lines.append('  %s()' % self.name)
            lines.append(']]>')
            lines.append(_indent + '</script>')
        if self.requirements:
            lines.append(_indent + '<depends>')
            for process in self.requirements:
                if process.supertask is not None:
                    process_name = '.'.join((process.supertask, process.name))
                else:
                    process_name = process.name
                lines.append(2*_indent +
                             ('<after process="%s"/>' % process_name))
            lines.append(_indent + '</depends>')
        if self.subtasks:
            lines.append(_indent + '<createsSubtasks>')
            for subtask in self.subtasks:
                lines.append(2*_indent + ('<subtask>%s</subtask>'
                                          % subtask.name))
            lines.append(_indent + '</createsSubtasks>')
        lines.append('</process>')
        if self.subtasks:
            lines.extend([str(subtask) for subtask in self.subtasks])
        return '\n'.join(lines)
