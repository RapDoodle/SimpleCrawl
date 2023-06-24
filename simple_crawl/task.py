import os
import uuid
import json
import pickle
from datetime import datetime
from collections import deque

class Task:
    def init(self, config):
        if hasattr(self, 'id') and self.id is not None:
            raise AssertionError('The task has been initialized. Double initialization is not allowed.')

        # Generate a random task id for the task
        self.id = uuid.uuid1().hex[:7]

        # Progress contains the current state of the task
        self.progress = {
            'id': self.id,
            'config': config,
            'created_at': datetime.utcnow(),
            'queue': deque(config['baseUrls']),
            'visited': set(),
            'failed': set(),
            'inserted': set()
        }

        # Save the task (first progress save)
        self.save_progress()
    
    def init_from_template(self, template_path):
        with open(template_path, 'r') as f:
            config = json.load(f)
        self.init(config)
        
    def save_progress(self, progress_path=''):
        with open(os.path.join(progress_path, f'{self.id}.scrawl'), 'wb') as f:
            pickle.dump(self.progress, f)

    def load_progress(self, progress_file_path):
        if hasattr(self, 'id') and self.id is not None:
            raise AssertionError('The task has been initialized. Double initialization is not allowed.')
        if not progress_file_path.endswith('.scrawl'):
            progress_file_path = f'{progress_file_path}.scrawl'
        with open(progress_file_path, 'rb') as f:
            self.progress = pickle.load(f)
        self.id = self.progress['id']