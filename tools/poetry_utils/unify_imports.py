import os
from tempfile import TemporaryDirectory
from benvy.databricks.repos.uploader.get_uploader import get_uploader
from daipecore.shortcut.ShortcutCreator import ShortcutCreator


def unify_imports():
    def project_root_path():
        return "/".join(os.path.normpath(os.getcwd()).split(os.sep)[:5]).lstrip("/Workspace/")

    with TemporaryDirectory() as tmp_dir:
        dst_dir = f"{project_root_path()}/src"
        ShortcutCreator().prepare_daipe_py(tmp_dir)
        get_uploader().upload_files_to_repo(f"{tmp_dir}/daipe.py", dst_dir)
