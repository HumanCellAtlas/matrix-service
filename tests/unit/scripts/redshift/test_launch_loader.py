import mock
import unittest

from scripts.redshift.launch_loader import launch_loader, _init_env_vars


class TestLaunchLoader(unittest.TestCase):
    def setUp(self):
        self.default_args = TestLaunchLoader.ArgsStub(instance_name="",
                                                      instance_type="test_type",
                                                      max_workers=1,
                                                      state=0,
                                                      s3_upload_id=None,
                                                      project_uuids=None,
                                                      bundle_fqids=None)

    @mock.patch("subprocess.call")
    @mock.patch("os.chdir")
    def test_init_env_vars(self, mock_chdir, mock_call):
        _init_env_vars()
        mock_call.assert_called_once_with("source environment", shell=True)

    @mock.patch("scripts.redshift.ec2_instance_manager.EC2InstanceManager.run")
    @mock.patch("scripts.redshift.ec2_instance_manager.EC2InstanceManager.provision")
    @mock.patch("scripts.redshift.ec2_instance_manager.EC2InstanceManager.clear_dir")
    @mock.patch("scripts.redshift.ec2_instance_manager.EC2InstanceManager.create")
    @mock.patch("scripts.redshift.ec2_instance_manager.EC2InstanceManager._fetch_account_id")
    @mock.patch("scripts.redshift.ec2_instance_manager.EC2InstanceManager.__init__")
    @mock.patch("scripts.redshift.launch_loader._init_env_vars")
    def test_launch_loader__new_instance(self,
                                         mock_init_env_vars,
                                         mock_ec2_init,
                                         mock_fetch_account_id,
                                         mock_create,
                                         mock_clear_dir,
                                         mock_provision,
                                         mock_run):
        args = self.default_args
        mock_ec2_init.return_value = None
        launch_loader(args)

        mock_init_env_vars.assert_called_once()
        mock_ec2_init.assert_called_once_with(name=mock.ANY)
        mock_create.assert_called_once_with(instance_type=args.instance_type)
        mock_clear_dir.assert_called_once_with("/mnt/*")
        mock_provision.assert_called_once()
        mock_run.assert_called_once_with(max_workers=args.max_workers,
                                         state=args.state,
                                         s3_upload_id=args.s3_upload_id,
                                         project_uuids=args.project_uuids,
                                         bundle_fqids=args.bundle_fqids)

    @mock.patch("scripts.redshift.ec2_instance_manager.EC2InstanceManager.run")
    @mock.patch("scripts.redshift.ec2_instance_manager.EC2InstanceManager.provision")
    @mock.patch("scripts.redshift.ec2_instance_manager.EC2InstanceManager.clear_dir")
    @mock.patch("scripts.redshift.ec2_instance_manager.EC2InstanceManager.create")
    @mock.patch("scripts.redshift.ec2_instance_manager.EC2InstanceManager._fetch_account_id")
    @mock.patch("scripts.redshift.ec2_instance_manager.EC2InstanceManager.__init__")
    @mock.patch("scripts.redshift.launch_loader._init_env_vars")
    def test_launch_loader__existing_instance(self,
                                              mock_init_env_vars,
                                              mock_ec2_init,
                                              mock_fetch_account_id,
                                              mock_create,
                                              mock_clear_dir,
                                              mock_provision,
                                              mock_run):
        args = self.default_args
        args.instance_name = "test_name"
        mock_ec2_init.return_value = None
        launch_loader(args)

        mock_init_env_vars.assert_called_once()
        mock_ec2_init.assert_called_once_with(name=args.instance_name)
        mock_create.assert_not_called()
        mock_clear_dir.assert_called_once_with("/mnt/*")
        mock_provision.assert_called_once()
        mock_run.assert_called_once_with(max_workers=args.max_workers,
                                         state=args.state,
                                         s3_upload_id=args.s3_upload_id,
                                         project_uuids=args.project_uuids,
                                         bundle_fqids=args.bundle_fqids)

    @mock.patch("scripts.redshift.ec2_instance_manager.EC2InstanceManager.run")
    @mock.patch("scripts.redshift.ec2_instance_manager.EC2InstanceManager.provision")
    @mock.patch("scripts.redshift.ec2_instance_manager.EC2InstanceManager.clear_dir")
    @mock.patch("scripts.redshift.ec2_instance_manager.EC2InstanceManager.create")
    @mock.patch("scripts.redshift.ec2_instance_manager.EC2InstanceManager._fetch_account_id")
    @mock.patch("scripts.redshift.ec2_instance_manager.EC2InstanceManager.__init__")
    @mock.patch("scripts.redshift.launch_loader._init_env_vars")
    def test_launch_loader__skip_download(self,
                                          mock_init_env_vars,
                                          mock_ec2_init,
                                          mock_fetch_account_id,
                                          mock_create,
                                          mock_clear_dir,
                                          mock_provision,
                                          mock_run):
        args = self.default_args
        args.instance_name = "test_name"
        args.state = 1
        mock_ec2_init.return_value = None
        launch_loader(args)

        mock_init_env_vars.assert_called_once()
        mock_ec2_init.assert_called_once_with(name=args.instance_name)
        mock_create.assert_not_called()
        mock_clear_dir.assert_called_once_with("/mnt/output/*")
        mock_provision.assert_called_once()
        mock_run.assert_called_once_with(max_workers=args.max_workers,
                                         state=args.state,
                                         s3_upload_id=args.s3_upload_id,
                                         project_uuids=args.project_uuids,
                                         bundle_fqids=args.bundle_fqids)

    class ArgsStub:
        def __init__(self,
                     instance_name,
                     instance_type,
                     max_workers,
                     state,
                     s3_upload_id,
                     project_uuids,
                     bundle_fqids):
            self.instance_name = instance_name
            self.instance_type = instance_type
            self.max_workers = max_workers
            self.state = state
            self.s3_upload_id = s3_upload_id
            self.project_uuids = project_uuids
            self.bundle_fqids = bundle_fqids
