import posixpath
from typing import Any, Callable

from file_system import FileSystem, FileSystemEvent, FileSystemEventType

TASK_NUM = 5

class ReplicatorSource:
    """Class representing the source side of a file replicator."""

    def __init__(self, fs: FileSystem, dir_path: str, rpc_handle: Callable[[Any], Any]):
        self._fs = fs
        self._dir_path = dir_path
        self._rpc_handle = rpc_handle

        self._watch_dirs = set()
        self._watch_files = set()


        self._watch_dirs.add(dir_path)

        # Start watching the directory for events
        self._fs.watchdir(self._dir_path, self.handle_event)

         # Trigger initial replication of the directory structure
        self._replicate_directory(self._dir_path)

        request = {
                    'action': 'sync',
                    'dirs': self._watch_dirs,
                    'files': self._watch_files,
                    'relative_path': '',

        }
        self._rpc_handle(request)

    def _replicate_directory(self, current_path: str):
        """Recursively replicate directories and files from the source to the target."""
        # List all items in the current directory
        for item in self._fs.listdir(current_path):
            item_path = posixpath.join(current_path, item)
            relative_path = posixpath.relpath(item_path, self._dir_path)

            # print(item_path, self._fs.isdir(item_path))

            if self._fs.isdir(item_path):
                # If the item is a directory, create it on the target
                request = {
                    'action': 'mkdir',
                    'relative_path': relative_path,
                }
                self._rpc_handle(request)

                self._watch_dirs.add(relative_path)

                self._fs.watchdir(item_path, self.handle_event)
                # print("Watching dirs",item_path)

                # Recursively replicate the contents of this directory
                self._replicate_directory(item_path)
            else:
                # If the item is a file, replicate the file
                content = self._fs.readfile(item_path)

                # print(content, relative_path)
                request = {
                    'action': 'write',
                    'relative_path': relative_path,
                    'content': content,
                }
                self._rpc_handle(request)
                self._watch_files.add(relative_path)


    def handle_event(self, event: FileSystemEvent):
        """Handle a file system event.

        Used as the callback provided to FileSystem.watchdir().
        """
        relative_path = posixpath.relpath(event.path, self._dir_path)

        # print("EVENT : ",event)

        if event.event_type == FileSystemEventType.FILE_OR_SUBDIR_ADDED:
            if self._fs.isdir(event.path):
                # Directory added
                request = {
                    'action': 'mkdir',
                    'relative_path': relative_path,
                }

                self._watch_dirs.add(relative_path)

                self._fs.watchdir(event.path, self.handle_event)
                # print("Watching dirs",event.path)

                self._replicate_directory(event.path)
            else:
                # File added
                content = self._fs.readfile(event.path)
                request = {
                    'action': 'write',
                    'relative_path': relative_path,
                    'content': content
                }
            self._rpc_handle(request)

            self._watch_files.add(relative_path)

        elif event.event_type == FileSystemEventType.FILE_MODIFIED:
            # File modified
            content = self._fs.readfile(event.path)
            request = {
                'action': 'write',
                'relative_path': relative_path,
                'content': content
            }
            self._rpc_handle(request)

        elif event.event_type == FileSystemEventType.FILE_OR_SUBDIR_REMOVED:
            
            remove_paths = []
            for path in self._watch_dirs:
                if path.startswith(relative_path):
                    base_path = posixpath.join(self._dir_path, path)
                    try:
                        self._fs.unwatchdir(base_path)
                    except:
                        pass
                        # print(path, "Directory not found")
                    remove_paths.append(path)

            for path in remove_paths:
                self._watch_dirs.discard(path)
            
            for path in self._watch_files:
                if path == relative_path:
                    self._watch_files.discard(path)
                    break


            # print("Watch Dirs local",self._watch_dirs,event.path)
            

            # File or directory removed
            request = {
                'action': 'remove',
                'relative_path': relative_path
            }
            self._rpc_handle(request)
            
            


class ReplicatorTarget:
    """Class representing the target side of a file replicator."""

    def __init__(self, fs: FileSystem, dir_path: str):
        self._fs = fs
        self._dir_path = dir_path

        # print(dir_path,"target")

        # self._fs.removedir(dir_path)
            
        # print("Removed all folders and files from target")
        

    def handle_request(self, request: Any) -> Any:
        """Handle a request from the ReplicatorSource."""
        action = request.get('action')
        relative_path = request.get('relative_path')
        target_path = posixpath.join(self._dir_path, relative_path)

        # print("REQUEST:", request)

        if action == 'write':
            # Write the content to the target file
            content = request.get('content')
            target_dir = posixpath.dirname(target_path)
            # print("target",target_path)
            if not self._fs.exists(target_dir):
                self._fs.makedirs(target_dir)
            try:
                if self._fs.isdir(target_path):
                    self._fs.removedir(target_path)
            except:
                # print(target_path, "is not a directory")
                pass
            
            if not self._fs.exists(target_path) or (self._fs.exists(target_path) and self._fs.readfile(target_path) != content):
                    self._fs.writefile(target_path, content)

        elif action == 'mkdir':
            # Create a directory in the target directory
            try:
                if not self._fs.isdir(target_path):
                    self._fs.removefile(target_path)
            except:
                # print(target_path, "is not a file")
                pass

            if not self._fs.exists(target_path):
                self._fs.makedirs(target_path)

        elif action == 'remove':
            # Remove the file or directory in the target directory
            if self._fs.exists(target_path):
                if self._fs.isdir(target_path):
                    self._fs.removedir(target_path)
                else:
                    self._fs.removefile(target_path)

        elif action == 'sync':
            # Remove the file or directory in the target directory
            dirs = request.get('dirs')
            files = request.get('files')
            self._sync_directory(self._dir_path,dirs,files)

            # print(type(dirs),files)


    def _sync_directory(self, current_path: str,dirs: set, files: set):
        """Recursively replicate directories and files from the source to the target."""
        # List all items in the current directory
        for item in self._fs.listdir(current_path):
            item_path = posixpath.join(current_path, item)
            relative_path = posixpath.relpath(item_path, self._dir_path)
            target_path = posixpath.join(self._dir_path, relative_path)

            if self._fs.isdir(item_path):
                if relative_path not in dirs:
                    self._fs.removedir(item_path)
                else:
                    self._sync_directory(item_path,dirs,files)
            else:
                if relative_path not in files:
                    self._fs.removefile(item_path)
