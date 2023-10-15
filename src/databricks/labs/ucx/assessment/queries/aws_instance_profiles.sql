-- viz type=table, name=AWS Instance Profiles, columns=instance_profile_arn,iam_role_arn,is_meta_instance_profile
-- widget title=AWS Instance Profiles, col=0, row=49, size_x=6, size_y=8
select
  instance_profile_arn,
  iam_role_arn,
  is_meta_instance_profile
from
  $inventory.aws_instance_profiles