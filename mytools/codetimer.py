from datetime import datetime

class CodeTimer(object):
    
    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.inspect_time = None
        self.time_span = None
        
        self.recoder = {}
        
    def start(self):
        self.start_time = datetime.now()
        self.inspect_time = self.start_time
        print 'CodeTimer started at: {0}'.format(self.start_time)
        
    def inspect(self, msg=''):
        current_time = datetime.now()
        if msg:
            msg = 'Inspect Message: {0:23.23s}'.format(msg)
        print msg+'CodeTimer Inspect at: {0}, Time Span is: {1}'.format(current_time,
                                                                    current_time-self.inspect_time
                                                                    )
        self.inspect_time = current_time 

    def set_tags(self, tags):
        for tag in tags:
            self.recoder[tag] = [None, None]
        
    def set_record(self, tag):
        if tag not in self.recoder:
            raise AssertionError('Tag "{0}" is not defined.'.format(tag))

        self.recoder[tag][0] = datetime.now()
        
    def get_record(self, tag):
        if tag not in self.recoder:
            raise AssertionError('Tag {0} is not defined.')
        
        current_time = datetime.now()
        if self.recoder[tag][1]:
            self.recoder[tag][1] += current_time-self.recoder[tag][0]
        else:
            self.recoder[tag][1] = current_time-self.recoder[tag][0]
        
    def refresh_recorder(self):
        print '== Codetimer Recorder: ======================================='
        _keys = self.recoder.keys()
        _keys.sort()
        for tag in _keys:
            print '{0:30.30s} CostTime: {1}'.format(tag, self.recoder[tag][1])
        print '=============================================================='
        
        self.recoder = {}
        
    def stop(self):
        self.end_time = datetime.now()
        print 'CodeTimer stopped at: {0}'.format(self.end_time)
        print 'Time Span is: {0}'.format(self.end_time-self.start_time)
        
        
codetimer = CodeTimer()    
        
        
if __name__ == '__main__':
    import time
    codetimer.start()
    codetimer.set_tags(tags=['Tag1', 'Tag2'])
    
    codetimer.set_record('Tag1')
    time.sleep(2)
    codetimer.get_record('Tag1')
    
    codetimer.set_record('Tag2')
    time.sleep(3)
    codetimer.get_record('Tag2')
    
    codetimer.set_record('Tag1')
    time.sleep(4)
    codetimer.get_record('Tag1')
    
    codetimer.refresh_recorder()
    codetimer.stop()
    