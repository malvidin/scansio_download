"""
Tracks files downloaded from scans.io, or other locations that use the same JSON catalog format.
Optionally interacts with Elastic Search to index file contents and track their result
Compressed file types currently observed are GZ, BZ2, and LZ4. A few studies are not compressed.
"""

__author__ = 'malvidin'
__version__ = '0.2'


from core import *
