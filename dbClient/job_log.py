from .conf import project_dir
import os


LOGGING = {
    'version': 1,
    'formatters': {
        'local': {
            'format': '%(asctime)s - %(levelname)s: %(message)s',
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'local',
        },
        'file': {
            'class': 'logging.FileHandler',
            'filename': os.path.join(project_dir, 'log', 'error.log'),
            'formatter': 'local',
        },
    },
    'root': {
        'handlers': ['console', 'file'],
    }
}

