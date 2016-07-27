import os
from collections import OrderedDict
from xml.dom import minidom

__all__ = ['Pipeline', 'Task', 'MainTask', 'Process']

def check_name(name):
    if len(name) > 30:
        raise ValueError('Process or task name must be 30 characters or fewer')

def job_line(job_type):
    if job_type is None:
        return None
    if job_type == 'long':
        return '<job maxCPU="${MAXCPULONG}" batchOptions="${BATCH_OPTIONS}" executable="${SCRIPT_LOCATION}/${BATCH_NAME}"/>'
    return '<job maxCPU="${MAXCPU}" batchOptions="${BATCH_OPTIONS}" executable="${SCRIPT_LOCATION}/${BATCH_NAME}"/>'

def data_path(filename):
    return os.path.join(os.environ['WORKFLOW_ENGINE_DIR'], 'data', filename)

class Pipeline(object):
    def __init__(self, name, version, pipeline_header=None):
        self.main_task = MainTask(name, version)
        self._read_pipeline_header(pipeline_header)

    def _read_pipeline_header(self, pipeline_header):
        if pipeline_header is None:
            pipeline_header = data_path('slac_pipeline_header.txt')
        with open(pipeline_header) as input_:
            self.header = ''.join(input_.readlines()).strip()

    def __str__(self):
        lines = [self.header, str(self.main_task), '</pipeline>']
        return '\n'.join(lines)

    def toxml(self, encoding='UTF-8', newl='', indent=4*' '):
        doc = minidom.parseString(str(self))
        return doc.toprettyxml(encoding=encoding, newl=newl, indent=indent)

class Task(object):
    def __init__(self, name, version=None):
        check_name(name)
        self.name = name
        self.version = version
        self.notation = None
        self.variable_lines = []
        self.processes = OrderedDict()

    def set_variables(self, varfile=None):
        if varfile is None:
            varfile = data_path('main_task_variables.txt')
        with open(varfile) as input_:
            self.variable_lines = [x.strip() for x in input_]

    def add_process(self, process):
        self.processes[process.name] = process
        if process.supertask is not None:
            raise RuntimeError("Cannot assign a process to more than one task")
        if self.version is None:
            process.supertask = self.name

    def create_process(self, process_name, job_type=None, requirements=[]):
        process = Process(process_name)
        process.job = job_line(job_type)
        self.add_process(process)
        return process

    def create_parallel_process(self, process_name, job_type, requirements=[]):
        if job_type is None:
            raise ValueError("job_type for a parallel process must be 'long' or 'std'")
        setup_process = Process('setup_' + process_name + 's')
        self.add_process(setup_process)
        for process in requirements:
            setup_process.requires(process)
        subtask = Task(process_name + 'sTask')
        setup_process.add_subtask(subtask)
        inner_process = Process(process_name)
        inner_process.job = job_line(job_type)
        subtask.add_process(inner_process)
        return inner_process

    def _header_lines(self):
        return ['<task name="%s" type="LSST">' % self.name]

    def __str__(self):
        lines = []
        lines.extend(self._header_lines())
        lines.extend(self.variable_lines)
        lines.extend([str(process) for process in self.processes.values()])
        lines.append('</task>')
        return '\n'.join(lines)

class MainTask(Task):
    def __init__(self, name, version):
        super(MainTask, self).__init__(name)
        self.version = version

    def _header_lines(self):
        return ['<task name="%s" type="LSST" version="%s">'
                % (self.name, self.version)]

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

    def _script_lines(self):
        lines = []
        lines.append('<script><![CDATA[')
        lines.append('    execfile("%s/%s" % (SLAC_SCRIPT_LOCATION, SCRIPT_NAME))')
        lines.append('    %s()' % self.name)
        lines.append(']]>')
        lines.append('</script>')
        return lines

    def _requirements_lines(self):
        lines = []
        if self.requirements:
            lines.append('<depends>')
            for process in self.requirements:
                if process.supertask is not None:
                    process_name = '.'.join((process.supertask, process.name))
                else:
                    process_name = process.name
                lines.append(('<after process="%s"/>' % process_name))
            lines.append('</depends>')
        return lines

    def _subtask_lines(self):
        lines = []
        if self.subtasks:
            lines.append('<createsSubtasks>')
            for subtask in self.subtasks:
                lines.append('<subtask>%s</subtask>' % subtask.name)
            lines.append('</createsSubtasks>')
        lines.append('</process>')
        if self.subtasks:
            lines.extend([str(subtask) for subtask in self.subtasks])
        return lines

    def __str__(self):
        lines = []
        lines.append('<process name="%s" site="${JOBSITE}">' % self.name)
        if self.notation is not None:
            lines.append('%s<notation>%s</notation>' % (_indent, self.notation))
        # A process comprises only either a job or a script.
        if self.job is not None:
            lines.append(self.job)
        else:
            lines.extend(self._script_lines())
        lines.extend(self._requirements_lines())
        lines.extend(self._subtask_lines())
        return '\n'.join(lines)
