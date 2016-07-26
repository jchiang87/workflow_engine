_indent = ' '*4

def job():
    return '<job maxCPU="${MAXCPU}" batchOptions="${BATCH_OPTIONS}" executable="${SCRIPT_LOCATION}/${BATCH_NAME}"/>'

def long_job():
    return '<job maxCPU="${MAXCPULONG}" batchOptions="${BATCH_OPTIONS}" executable="${SCRIPT_LOCATION}/${BATCH_NAME}"/>'

class Pipeline(object):
    def __init__(self, name):
        self.main_task = Task(name)

    def __str__(self):
        return str(self.main_task)

class Task(object):
    def __init__(self, name):
        self.name = name
        self.processes = {}

    def set_variables(self, vars):
        for key, value in vars:
            self.set_variable(key, value)

    def set_variable(self, key, value):
        pass

    def add_process(self, process):
        self.processes[process.name] = process
        if process.superTask is not None:
            raise RuntimeError("Cannot assign a process to more than one task")
        process.supertask = self.name

    def __str__(self):
        lines = [str(process) for process in self.processes.values()]
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
            lines.append(_indent + '<notation>' + self.notation '</notation>')
        # A process comprises either a job or a script.
        if self.job is not None:
            lines.append(_indent + self.job)
        else:
            lines.append(_indent + '<script><![CDATA[')
            lines.append('  execfile("%s/%s" % (SLAC_SCRIPT_LOCATION, SCRIPT_NAME))')
            lines.append('  %s()' % self.name)
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
                lines.append(2*_indent + ('<subtask>%s</subtask>' % subtask))
            lines.append(_indent + '</createsSubtasks>')
        lines.append('</process>')
        return '\n'.join(lines)
