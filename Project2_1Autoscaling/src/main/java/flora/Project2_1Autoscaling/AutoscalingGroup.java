package flora.Project2_1Autoscaling;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.autoscaling.AmazonAutoScaling;
import com.amazonaws.services.autoscaling.AmazonAutoScalingClient;
import com.amazonaws.services.autoscaling.model.CreateAutoScalingGroupRequest;
import com.amazonaws.services.autoscaling.model.CreateAutoScalingGroupResult;
import com.amazonaws.services.autoscaling.model.CreateLaunchConfigurationRequest;
import com.amazonaws.services.autoscaling.model.CreateLaunchConfigurationResult;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingInstancesRequest;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingInstancesResult;
import com.amazonaws.services.autoscaling.model.EnableMetricsCollectionRequest;
import com.amazonaws.services.autoscaling.model.EnableMetricsCollectionResult;
import com.amazonaws.services.autoscaling.model.InstanceMonitoring;
import com.amazonaws.services.autoscaling.model.PutScalingPolicyRequest;
import com.amazonaws.services.autoscaling.model.PutScalingPolicyResult;
import com.amazonaws.services.autoscaling.model.Tag;
import com.amazonaws.services.autoscaling.model.UpdateAutoScalingGroupRequest;
import com.amazonaws.services.autoscaling.model.UpdateAutoScalingGroupResult;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.PutMetricAlarmRequest;
import com.amazonaws.services.cloudwatch.model.PutMetricAlarmResult;

public class AutoscalingGroup {

	public static void Launchconfig(String secid, String type, String name) {
		AWSCredentials credentials = null;
		try {
			credentials = new ProfileCredentialsProvider().getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
					+ "Please make sure that your credentials file is at the correct "
					+ "location (~/.aws/credentials), and is in valid format.", e);
		}
		AmazonAutoScaling client = new AmazonAutoScalingClient(credentials);
		CreateLaunchConfigurationRequest request = new CreateLaunchConfigurationRequest().withImageId("ami-2fca3739")
				.withInstanceType(type).withLaunchConfigurationName(name).withSecurityGroups(secid)
				.withInstanceMonitoring(new InstanceMonitoring().withEnabled(true));
		CreateLaunchConfigurationResult response = client.createLaunchConfiguration(request);
		System.out.println(response.toString());

	}

	public static void Asgroup() {
		AWSCredentials credentials = null;
		Tag tag = new Tag();
		tag.setKey("Project");
		tag.setValue("2.1");
		try {
			credentials = new ProfileCredentialsProvider().getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
					+ "Please make sure that your credentials file is at the correct "
					+ "location (~/.aws/credentials), and is in valid format.", e);
		}
		AmazonAutoScaling client = new AmazonAutoScalingClient(credentials);
		CreateAutoScalingGroupRequest request = new CreateAutoScalingGroupRequest().withAvailabilityZones("us-east-1c")
				.withHealthCheckGracePeriod(60).withHealthCheckType("ELB")
				.withLaunchConfigurationName("my-launch-config").withDefaultCooldown(60).withLoadBalancerNames("my-elb")
				.withAutoScalingGroupName("my-auto-scaling-group").withMaxSize(5).withMinSize(2)
				.withTags(tag).withVPCZoneIdentifier("subnet-c49ee6e9");
		CreateAutoScalingGroupResult response = client.createAutoScalingGroup(request);
		System.out.println(response.toString());

		EnableMetricsCollectionRequest cwrequest = new EnableMetricsCollectionRequest()
				.withAutoScalingGroupName("my-auto-scaling-group").withGranularity("1Minute");
		EnableMetricsCollectionResult cwresponse = client.enableMetricsCollection(cwrequest);

		PutScalingPolicyRequest policyrequest1 = new PutScalingPolicyRequest().withAdjustmentType("ChangeInCapacity")
				.withAutoScalingGroupName("my-auto-scaling-group").withPolicyName("Scalein").withScalingAdjustment(-1)
				.withCooldown(60);
		PutScalingPolicyResult policyresponse1 = client.putScalingPolicy(policyrequest1);

		PutScalingPolicyRequest policyrequest2 = new PutScalingPolicyRequest().withAdjustmentType("ChangeInCapacity")
				.withAutoScalingGroupName("my-auto-scaling-group").withPolicyName("Scaleout").withScalingAdjustment(1)
				.withCooldown(80);
		PutScalingPolicyResult policyresponse2 = client.putScalingPolicy(policyrequest2);
		String arn1 = policyresponse1.toString();
		String arn2 = policyresponse2.toString();
		AmazonCloudWatch cwclient = new AmazonCloudWatchClient(credentials);
		Dimension dimension=new Dimension().withName("AutoScalingGroupName").withValue("my-auto-scaling-group");
		PutMetricAlarmRequest alarmRequest1 = new PutMetricAlarmRequest().withAlarmName("scaleout")
				.withComparisonOperator("GreaterThanOrEqualToThreshold").withEvaluationPeriods(1).withDimensions(dimension)
				.withMetricName("CPUUtilization").withNamespace("AWS/EC2").withPeriod(60).withStatistic("Average")
				.withThreshold(57.0).withAlarmActions(arn2.substring(arn2.indexOf("arn:")));
		PutMetricAlarmResult alarmresult1 = cwclient.putMetricAlarm(alarmRequest1);
		PutMetricAlarmRequest alarmRequest2 = new PutMetricAlarmRequest().withAlarmName("scalein")
				.withComparisonOperator("LessThanOrEqualToThreshold").withEvaluationPeriods(1).withDimensions(dimension)
				.withMetricName("CPUUtilization").withNamespace("AWS/EC2").withPeriod(60).withStatistic("Average")
				.withThreshold(20.0).withAlarmActions(arn1.substring(arn1.indexOf("arn:")));
		PutMetricAlarmResult alarmresult2 = cwclient.putMetricAlarm(alarmRequest2);
	}

	public static void modifyAS(String secid, String type, String name) {
		AWSCredentials credentials = null;
		try {
			credentials = new ProfileCredentialsProvider().getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
					+ "Please make sure that your credentials file is at the correct "
					+ "location (~/.aws/credentials), and is in valid format.", e);
		}
		AmazonAutoScaling client = new AmazonAutoScalingClient(credentials);
		/*
		 * DescribeScheduledActionsRequest request = new
		 * DescribeScheduledActionsRequest()
		 * .withAutoScalingGroupName("my-auto-scaling-group");
		 * DescribeScheduledActionsResult response = client
		 * .describeScheduledActions(request);
		 */
		Launchconfig(secid, type, name);
		UpdateAutoScalingGroupRequest updateRequest = new UpdateAutoScalingGroupRequest()
				.withAutoScalingGroupName("my-auto-scaling-group").withLaunchConfigurationName(name);
		UpdateAutoScalingGroupResult updateResult = client.updateAutoScalingGroup(updateRequest);
	}

	public static void describeins() {
		AWSCredentials credentials = null;
		try {
			credentials = new ProfileCredentialsProvider().getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
					+ "Please make sure that your credentials file is at the correct "
					+ "location (~/.aws/credentials), and is in valid format.", e);
		}
		AmazonAutoScaling client = new AmazonAutoScalingClient(credentials);
		DescribeAutoScalingInstancesRequest desins = new DescribeAutoScalingInstancesRequest();
		DescribeAutoScalingInstancesResult insres = client.describeAutoScalingInstances(desins);
		System.out.println(insres.toString());
	}
}
