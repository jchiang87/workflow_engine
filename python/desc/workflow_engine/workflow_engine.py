from __future__ import print_function, absolute_import
import os
from collections import OrderedDict
from xml.dom import minidom

__all__ = ['Pipeline', 'Task', 'MainTask', 'Process', 'package_data_path']

class Pipeline(object):
    def __init__(self, name, version, pipeline_header=None):
        self.main_task = MainTask(name, version)
        self._read_pipeline_header(pipeline_header)

    def toxml(self, encoding='UTF-8', newl='', indent=4*' '):
        doc = minidom.parseString(str(self))
        return doc.toprettyxml(encoding=encoding, newl=newl, indent=indent)

    def write_python_module(self, clobber=False):
        script_name = self.get_module_name()
        if os.path.isfile(script_name) and not clobber:
            return
        with open(script_name, 'w') as output:
            # Extract parent and child process names for parallelized
            # tasks.
            subtask_names = []
            for process in self.main_task.processes:
                if process.subtasks:
                    for subtask in process.subtasks:
                        subtask_names.append((process.name, subtask.name))
                        for subprocess in subtask.processes:
                            if subprocess.job is None:
                                self._write_function(output, subprocess.name)
                elif process.job is None:
                    self._write_function(output, process.name)
            # Write boilerplate empty lists of parallelizeable tasks.
            for outer_process, subtask_name in subtask_names:
                self._write_job_list(output, subtask_name)
            # Write stream launching functions.
            for outer_process, subtask_name in subtask_names:
                self._write_stream_launching_function(output, outer_process,
                                                      subtask_name)

    def write_process_scripts(self, clobber=False):
        num_scripts = 0
        for process in self.main_task.processes:
            if process.subtasks:
                for subtask in process.subtasks:
                    for subprocess in subtask.processes:
                        if subprocess.job is not None:
                            self._create_process_script(subprocess.name,
                                                        clobber)
                            num_scripts += 1
            elif process.job is not None:
                self._create_process_script(process.name, clobber)
                num_scripts += 1
        return num_scripts

    @staticmethod
    def _create_process_script(process_name, clobber):
        if os.path.isfile(process_name) and not clobber:
            return
        with open(process_name, 'w') as output:
            output.write('echo "Running %s."\n' % process_name)

    @staticmethod
    def _write_function(output, process_name):
        output.write("""def %(process_name)s():
    pass

""" % locals())

    @staticmethod
    def _write_job_list(output, subtask_name):
        output.write("%(subtask_name)s_jobs = []\n" % locals())

    @staticmethod
    def _write_stream_launching_function(output, setup_process_name,
                                         subtask_name):
        output.write("""
def %(setup_process_name)s():
    for i, job in enumerate(%(subtask_name)s_jobs):
        pipeline.createSubstream("%(subtask_name)s", i, job.pipeline_vars)
""" % locals())

    def get_module_name(self):
        doc = minidom.parseString(str(self))
        vars = doc.getElementsByTagName('var')
        for var in vars:
            if var.getAttribute('name') == 'SCRIPT_NAME':
                return var.firstChild.nodeValue

    def _read_pipeline_header(self, pipeline_header):
        if pipeline_header is None:
            pipeline_header = package_data_path('slac_pipeline_header.txt')
        with open(pipeline_header) as input_:
            self.header = ''.join(input_.readlines()).strip()

    def __str__(self):
        lines = [self.header, str(self.main_task), '</pipeline>']
        return '\n'.join(lines)

class Task(object):
    def __init__(self, name, version=None):
        check_name(name)
        self.name = name
        self.version = version
        self.notation = None
        self.outer_process = None
        self.variable_lines = []
        self.processes = []

    def set_variables(self, varfile=None):
        if varfile is None:
            varfile = package_data_path('main_task_variables.txt')
        with open(varfile) as input_:
            self.variable_lines = [x.strip() for x in input_]

    def get_variable(self, varname):
        line = self.variable_lines[self._get_variable_line_index(varname)]
        return line.split('>')[1].split('<')[0]

    def set_variable(self, varname, value):
        index = self._get_variable_line_index(varname)
        self.variable_lines[index] =\
            '<var name="%s">%s</var>' % (varname, value)

    def _get_variable_line_index(self, varname):
        for i, line in enumerate(self.variable_lines):
            if line.startswith('<var name="%s"' % varname):
                return i
        raise RuntimeError("variable %s not found" % varname)

    def add_process(self, process):
        self.processes.append(process)
        if process.owner_task is not None:
            raise RuntimeError("Cannot assign a process to more than one task")
        if self.version is None:
            process.owner_task = self

    def create_process(self, process_name, job_type='std', requirements=[]):
        process = Process(process_name)
        process.job = job_line[job_type]
        self.add_process(process)
        for item in requirements:
            process.requires(item)
        return process

    def create_parallel_process(self, process_name, job_type='std',
                                requirements=[]):
        if job_type not in ('std', 'long'):
            raise RuntimeError\
                ("job_type for a parallel process must be 'long' or 'std'")
        outer_process = Process('setup_' + process_name + 's')
        self.add_process(outer_process)
        for process in requirements:
            outer_process.requires(process)
        subtask = Task(process_name + 'sTask')
        outer_process.add_subtask(subtask)
        inner_process = Process(process_name)
        inner_process.job = job_line[job_type]
        subtask.add_process(inner_process)
        return outer_process

    def _header_lines(self):
        return ['<task name="%s" type="LSST">' % self.name]

    def __str__(self):
        lines = []
        lines.extend(self._header_lines())
        lines.extend(self.variable_lines)
        lines.extend([str(process) for process in self.processes])
        lines.append('</task>')
        return '\n'.join(lines)

class MainTask(Task):
    def __init__(self, name, version):
        super(MainTask, self).__init__(name)
        self.version = version

    def _header_lines(self):
        return ['<task name="%s" type="LSST" version="%s">'
                % (self.name, self.version)]

    def __str__(self):
        lines = []
        lines.extend(self._header_lines())
        lines.extend(self.variable_lines)
        lines.extend([str(process) for process in self.processes])
        for process in self.processes:
            for subtask in process.subtasks:
                lines.append(str(subtask))
        lines.append('</task>')
        return '\n'.join(lines)

class Process(object):
    def __init__(self, name):
        check_name(name)
        self.name = name
        self.notation = None
        self.job = None
        self.script = None
        self.requirements = []
        self.subtasks = []
        self.owner_task = None

    def requires(self, process):
        if not process.subtasks:
            self.requirements.append(process)
            return
        for subtask in process.subtasks:
            for subprocess in subtask.processes:
                self.requirements.append(subprocess)

    def add_subtask(self, task):
        self.subtasks.append(task)
        task.outer_process = self

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
                if process.owner_task is not None:
                    process_name = '.'.join((process.owner_task.name,
                                             process.name))
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
        return lines

    def __str__(self):
        lines = []
        lines.append('<process name="%s" site="${JOBSITE}">' % self.name)
        if self.notation is not None:
            lines.append('<notation>%s</notation>' % (self.notation))
        # A process comprises only either a job or a script.
        if self.job is not None:
            lines.append(self.job)
        else:
            lines.extend(self._script_lines())
        lines.extend(self._requirements_lines())
        lines.extend(self._subtask_lines())
        return '\n'.join(lines)

def package_data_path(filename):
    return os.path.join(os.environ['WORKFLOW_ENGINE_DIR'], 'data', filename)

def check_name(name):
    if len(name) > 30:
        raise RuntimeError\
            (name + ': process or task name must be 30 characters or fewer.')
    try:
        exec(name + ' = 1')
    except SyntaxError:
        raise RuntimeError('Invalid process or task name: ' + name)

job_line = dict([('script', None),
                 ('long', '<job maxCPU="${MAXCPULONG}" batchOptions="${BATCH_OPTIONS}" executable="${SCRIPT_LOCATION}/${BATCH_NAME}"/>'),
                 ('std', '<job maxCPU="${MAXCPU}" batchOptions="${BATCH_OPTIONS}" executable="${SCRIPT_LOCATION}/${BATCH_NAME}"/>')])
