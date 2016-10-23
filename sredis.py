# Homework
import re
import socket
import logging

logger = logging.getLogger('sRedis')
logger.setLevel(logging.INFO)


class RedisError(RuntimeError):
    pass


class DataParser(object):

    def __init__(self):
        self.result = None
        self.done = False           # use a flag to judge because parse result can be None
        self.stack = []             # store lists as a stack
        self.expected_length = -1   # -1 if expecting control characters

    def reset(self):
        self.result = None
        self.done = False
        self.stack = []
        self.expected_length = -1

    def compose(self, parts):
        data = '*{}\r\n'.format(len(parts))
        for part in parts:
            data += '${}\r\n{}\r\n'.format(len(part), part)
        return data

    def _check_end(self):
        if len(self.stack[-1][0]) == self.stack[-1][1]:
            item = self.stack.pop()[0]
            if len(self.stack) == 0:
                self.result = item
                self.done = True
            else:
                self.stack[-1][0].append(item)

    def _append_item(self, item):
        logger.debug('appending', item)
        if len(self.stack) == 0:
            self.result = item
            self.done = True
        else:
            self.stack[-1][0].append(item)
            self._check_end()
            logger.debug('current stack:', self.stack)

    def _consume(self, line):
        if self.done:
            raise SyntaxError('Too much data. Consume {} failed'.format(line))
        logger.debug('consuming', line)
        if self.expected_length == -1:
            prefix = line[0]
            if prefix == '+':
                self._append_item(line[1:])
            elif prefix == '-':
                raise RedisError(line[1:])
            elif prefix == ':':
                self._append_item(int(line[1:]))
            elif prefix == '$':
                str_length = int(line[1:])
                if str_length == -1:
                    self._append_item(None)
                else:
                    self.expected_length = int(line[1:])
            elif prefix == '*':
                array_length = int(line[1:])
                if array_length == 0:
                    self._append_item([])
                else:
                    self.stack.append(([], int(line[1:])))
                    logger.debug('current stack:', self.stack)
            else:
                raise SyntaxError('Unknown control character')
        else:
            # just append data to stack, only happens in bulk string
            if self.expected_length != len(line):
                raise SyntaxError('Wrong bulk string length.')
            self._append_item(line)
            self.expected_length = -1

    def feed(self, data):
        if not data.endswith('\r\n'):
            raise RuntimeError(r'Only data ends with \r\n can be feeded.')
        logger.debug('feed:\n', data)
        lines = data.split('\r\n')[:-1]
        for line in lines:
            logger.debug('yield', line)
            self._consume(line)


class Redis(object):

    command_pattern = re.compile(r'''\b(?:[^'" ]+)\b|'(?:[^']+)'|"(?:[^"]+)"''')

    def __init__(self, host='127.0.0.1', port=6379, db=0):
        self.connection = self._get_connection(host, port)
        self.parser = DataParser()

    def _get_connection(self, host, port):
        s = socket.socket()
        s.connect((host, port))
        return s

    @classmethod
    def _parse_command(cls, command):
        # A simple parse method, should have been changed
        if not isinstance(command, str):
            raise SyntaxError('Command should be a str!')
        if len(command) > 1024*1024:
            raise SyntaxError('Command too long!')
        return command.split()

    def execute(self, command):
        formatted_data = self.parser.compose(self._parse_command(command))
        self.connection.send(formatted_data)
        parts = []
        while True:
            part = self.connection.recv(8192)   # May be incompleted data if timed out
            parts.append(part)
            if part.endswith('\r\n'):   # not strict end condition
                data = ''.join(parts)
                self.parser.feed(data)
            if self.parser.done:        # Then end condition is strict
                result = self.parser.result
                self.parser.reset()
                return result, type(result)
            else:
                parts = []
                continue

if __name__ == '__main__':
    r = Redis(host='192.168.99.100')
    print r.execute('select 2')
    print '---'
    print r.execute('get a')
    print '---'
    print r.execute('incr a')
    print '---'
    print r.execute('get a')
    print '---'
    print r.execute('set a 2')
    print '---'
    print r.execute('get a')
    print '----'
    print r.execute('del a')
    print '---'
    print r.execute('sadd test 100 200 300')
    print '----'
    print r.execute('smembers test')
    print '----'
    print r.execute('del test')