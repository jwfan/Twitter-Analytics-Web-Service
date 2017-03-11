package lxfree.query1.autoscaling;


import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.autoscaling.AmazonAutoScaling;
import com.amazonaws.services.autoscaling.AmazonAutoScalingClientBuilder;
import com.amazonaws.services.autoscaling.model.CreateAutoScalingGroupRequest;
import com.amazonaws.services.autoscaling.model.CreateLaunchConfigurationRequest;
import com.amazonaws.services.autoscaling.model.DeleteAutoScalingGroupRequest;
import com.amazonaws.services.autoscaling.model.DeleteLaunchConfigurationRequest;
import com.amazonaws.services.autoscaling.model.InstanceMonitoring;
import com.amazonaws.services.autoscaling.model.PutScalingPolicyRequest;
import com.amazonaws.services.autoscaling.model.PutScalingPolicyResult;
import com.amazonaws.services.autoscaling.model.UpdateAutoScalingGroupRequest;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.PutMetricAlarmRequest;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.CreateSecurityGroupRequest;
import com.amazonaws.services.ec2.model.CreateSecurityGroupResult;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DeleteSecurityGroupRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusResult;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsRequest;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.IpPermission;
import com.amazonaws.services.ec2.model.IpRange;
import com.amazonaws.services.ec2.model.Placement;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancing;
import com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancingClientBuilder;
import com.amazonaws.services.elasticloadbalancing.model.CreateLoadBalancerRequest;
import com.amazonaws.services.elasticloadbalancing.model.DeleteLoadBalancerRequest;
import com.amazonaws.services.elasticloadbalancing.model.DescribeLoadBalancersRequest;
import com.amazonaws.services.elasticloadbalancing.model.DescribeLoadBalancersResult;
import com.amazonaws.services.elasticloadbalancing.model.Listener;

/**
 * This class will start the AWS auto scaling
 *
 * @author Ruixue
 *
 */
public class UndertowStartUp {

    protected static final String KEY_NAME = "CCNOBUG";
	protected static String INSTANCE_TYPE = "m3.medium";
    protected static AmazonEC2 ec2;
    private static final String UNDERTOW_IMAGE = "ami-dfc835c9";
    private static final String AV_ZONE = "us-east-1a";
    private static final String BALANCER_NAME = "auto-scaling-elb";
    private static String LOAD_BALANCER_DNS;
    private static final String LAUNCH_CONFIGURATION = "lc-autoscaling";
    private static final String AUTO_SCALING_NAME = "ac-group";
    private static final String SECURITY_GROUP = "autoscaling";

    public static void main(String[] args) {
        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider().getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
                    + "Please make sure that your credentials file is at the correct "
                    + "location (~/.aws/credentials), and is in valid format.", e);
        }

        // Create the AmazonEC2Client object so we can call various APIs.
        ec2 = AmazonEC2ClientBuilder.standard().withRegion(AV_ZONE)
                .withCredentials(new AWSStaticCredentialsProvider(credentials)).build();

        try {
            // Create new security group.
            String scGroupId = creatSecurityGroup(SECURITY_GROUP, "Query1 security group");

            //Create Load Balancer
            AmazonElasticLoadBalancing elbclient = AmazonElasticLoadBalancingClientBuilder.defaultClient();
            //Create listener
            Listener listener = new Listener();
            listener.withLoadBalancerPort(80).withProtocol("HTTP")
                    .withInstancePort(80).withInstanceProtocol("HTTP");

            //Create Load Balancer
            CreateLoadBalancerRequest lbrequest = new CreateLoadBalancerRequest()
                    .withLoadBalancerName(BALANCER_NAME)
                    .withListeners(listener)
                    .withAvailabilityZones(AV_ZONE)
                    .withTags(new com.amazonaws.services.elasticloadbalancing.model.Tag().withKey("Project").withValue("Phase1"))
                    .withSecurityGroups(scGroupId);
            elbclient.createLoadBalancer(lbrequest);
            System.out.println("Load Balancer Created!");
            
            //Get Load Balancer DNS
            DescribeLoadBalancersRequest lbRequest = new DescribeLoadBalancersRequest();
            List<String> blNameList = new ArrayList<String>();
            blNameList.add(BALANCER_NAME);
            lbRequest.setLoadBalancerNames(blNameList);
            DescribeLoadBalancersResult lbResult = elbclient.describeLoadBalancers(lbRequest);
            LOAD_BALANCER_DNS = lbResult.getLoadBalancerDescriptions().get(0).getDNSName();
            System.out.println("Load Balancer DNS: " + LOAD_BALANCER_DNS);
            
            //Create Launch Configuration
            AmazonAutoScaling asClient = AmazonAutoScalingClientBuilder.defaultClient();
            CreateLaunchConfigurationRequest lauchConfReq = new CreateLaunchConfigurationRequest();
            lauchConfReq.withImageId(UNDERTOW_IMAGE)
            .withInstanceType(INSTANCE_TYPE)
            .withLaunchConfigurationName(LAUNCH_CONFIGURATION)
            .withInstanceMonitoring(new InstanceMonitoring().withEnabled(true))
            .withSecurityGroups(SECURITY_GROUP)
            .withKeyName(KEY_NAME);
            asClient.createLaunchConfiguration(lauchConfReq);
            System.out.println("Launch configuration created!");
            
            //Create auto scaling group
            CreateAutoScalingGroupRequest autoSCReq = new CreateAutoScalingGroupRequest();
            autoSCReq.withAutoScalingGroupName(AUTO_SCALING_NAME)
            .withAvailabilityZones(AV_ZONE)
            .withDefaultCooldown(60)
            .withLoadBalancerNames(BALANCER_NAME)
            .withHealthCheckType("ELB")
            .withHealthCheckGracePeriod(200)
            .withLaunchConfigurationName(LAUNCH_CONFIGURATION)
            .withMaxSize(15)
            .withMinSize(5)
            .withTags(new com.amazonaws.services.autoscaling.model.Tag()
                            .withKey("Project").withValue("Phase1")
                            .withPropagateAtLaunch(true).withResourceId(AUTO_SCALING_NAME));
            asClient.createAutoScalingGroup(autoSCReq);
            System.out.println("Auto Scaling Group Created!");
            
            //Set auto scaling rules
            //Create scale out rule
            PutScalingPolicyRequest addOneInstance = new PutScalingPolicyRequest();
            addOneInstance.withPolicyName("addOneInstance")
            .withAdjustmentType("ChangeInCapacity")
            .withAutoScalingGroupName(AUTO_SCALING_NAME)
            .withCooldown(120)
            .withScalingAdjustment(1);
            PutScalingPolicyResult addOneInstanceResult = asClient.putScalingPolicy(addOneInstance);
            System.out.println("'Add one instance policy' for scale out is created!");
            
            //Create scale in rule
            PutScalingPolicyRequest reduceOneInstance = new PutScalingPolicyRequest();
            reduceOneInstance.withPolicyName("reduceOneInstance")
            .withAdjustmentType("ChangeInCapacity")
            .withAutoScalingGroupName(AUTO_SCALING_NAME)
            .withCooldown(120)
            .withScalingAdjustment(-1);

            PutScalingPolicyResult reduceOneResult = asClient.putScalingPolicy(reduceOneInstance);
            System.out.println("'Reduce one Instance' policy for scale out is created!");
            
            //Set cloud watch alarm for scale out
            AmazonCloudWatch  cwClient = AmazonCloudWatchClientBuilder.defaultClient();
            PutMetricAlarmRequest addOneInsAlarm = new PutMetricAlarmRequest();
            addOneInsAlarm.withActionsEnabled(true)
            .withAlarmName("addOneInstanceAlarm")
            .withAlarmActions(addOneInstanceResult.getPolicyARN())
            .withComparisonOperator("GreaterThanThreshold")
            .withDimensions(new Dimension().withName("AutoScalingGroupName").withValue(AUTO_SCALING_NAME))
            .withEvaluationPeriods(1)
            .withNamespace("AWS/EC2")
            .withMetricName("CPUUtilization")
            .withThreshold(58.0D)
            .withPeriod(60)
            .withStatistic("Average");
            cwClient.putMetricAlarm(addOneInsAlarm);
            System.out.println("Scale out alarm is created!");
            
            //Set cloud watch alarm for scale in
            PutMetricAlarmRequest reduceOneInsAlarm = new PutMetricAlarmRequest();
            reduceOneInsAlarm.withActionsEnabled(true)
            .withAlarmName("reduceOneInstanceAlarm")
            .withAlarmActions(reduceOneResult.getPolicyARN())
            .withComparisonOperator("LessThanThreshold")
            .withDimensions(new Dimension().withName("AutoScalingGroupName").withValue(AUTO_SCALING_NAME))
            .withEvaluationPeriods(1)
            .withNamespace("AWS/EC2")
            .withMetricName("CPUUtilization")
            .withThreshold(30.0D)
            .withPeriod(60)
            .withStatistic("Average");
            cwClient.putMetricAlarm(reduceOneInsAlarm);
            System.out.println("Scale in alarm is created!");

            
            //Terminate all resources
            UpdateAutoScalingGroupRequest updateASRequest = new UpdateAutoScalingGroupRequest();
            updateASRequest.withAutoScalingGroupName(AUTO_SCALING_NAME)
            .withDesiredCapacity(0)
            .withMaxSize(0)
            .withMinSize(0);
			
            asClient.updateAutoScalingGroup(updateASRequest);
            System.out.println("Set vm in auto scaling group to 0!");
            Thread.sleep(120000);
			
            // Delete atuo scaling group
            DeleteAutoScalingGroupRequest delASGRequest = new DeleteAutoScalingGroupRequest().withAutoScalingGroupName(AUTO_SCALING_NAME);
            asClient.deleteAutoScalingGroup(delASGRequest);
            System.out.println("Auto scaling group is deleted!");
            
            //Delete Load Balancer
            DeleteLoadBalancerRequest dellbReq = new DeleteLoadBalancerRequest().withLoadBalancerName(BALANCER_NAME);
            elbclient.deleteLoadBalancer(dellbReq);
            System.out.println("Load Balancer is deleted!");
			
            // Delete launch configuration
            DeleteLaunchConfigurationRequest delcRequest = new DeleteLaunchConfigurationRequest().withLaunchConfigurationName(LAUNCH_CONFIGURATION);
            asClient.deleteLaunchConfiguration(delcRequest);
            System.out.println("Launch configuration is deleted!");
            
            Thread.sleep(30000);
            
            //Delete scaling security group
            DeleteSecurityGroupRequest delSGReq = new DeleteSecurityGroupRequest().withGroupId(scGroupId);
            ec2.deleteSecurityGroup(delSGReq);
            System.out.println("Scaling security group is deleted!");
            
            System.out.println("Test end!");
            
        } catch (AmazonServiceException ase) {
            // Likely this means that the group is already created, so ignore.
            System.out.println(ase.getMessage());
        } catch (InterruptedException e) {
        }
    }

    private static String  creatSecurityGroup(String name, String description) throws InterruptedException {
        CreateSecurityGroupRequest csgrLG = new CreateSecurityGroupRequest();
        csgrLG.withGroupName(name).withDescription(description);
        CreateSecurityGroupResult createSecurityGroupResult = ec2.createSecurityGroup(csgrLG);
        //Set IP and port permission
        IpPermission ipPermission = new IpPermission();
        List<IpRange> ipList = new ArrayList<IpRange>();
        IpRange range = new IpRange();
        range.setCidrIp("0.0.0.0/0");
        ipList.add(range);
        ipPermission.withIpv4Ranges(ipList).withIpProtocol("tcp").withFromPort(80).withToPort(80);
        AuthorizeSecurityGroupIngressRequest authorizeSecurityGroupIngressRequest = new AuthorizeSecurityGroupIngressRequest();
        authorizeSecurityGroupIngressRequest.withGroupName(name).withIpPermissions(ipPermission);
        ec2.authorizeSecurityGroupIngress(authorizeSecurityGroupIngressRequest);
        System.out.println("Security group: " + name + " is created");
        DescribeSecurityGroupsRequest dsgReq = new DescribeSecurityGroupsRequest().withGroupNames(name);
        DescribeSecurityGroupsResult dsgRes = ec2.describeSecurityGroups(dsgReq);
        return dsgRes.getSecurityGroups().get(0).getGroupId();
    }

    public static String launchInstance(String imageID, String securityGroup) throws InterruptedException {
        //Launch instance
		RunInstancesRequest runInstancesRequest =
				   new RunInstancesRequest();
		Placement place = new Placement();
        place.setAvailabilityZone(AV_ZONE);
		
		runInstancesRequest.withImageId(imageID)
		                   .withInstanceType(INSTANCE_TYPE)
		                   .withMinCount(1)
		                   .withMaxCount(1)
		                   .withKeyName(KEY_NAME)
		                   .withSecurityGroups(securityGroup)
		                   .setPlacement(place);
							
		RunInstancesResult result = ec2.runInstances(runInstancesRequest);

        //Tag instance
        Instance instance = result.getReservation().getInstances().get(0);
        String instanceID = instance.getInstanceId();
        CreateTagsRequest createTagsRequest = new CreateTagsRequest();
        createTagsRequest.withResources(instanceID).withTags(new Tag("Project", "Phase1"));
        ec2.createTags(createTagsRequest);

        //Check instance status
        DescribeInstanceStatusRequest describeRequest = new DescribeInstanceStatusRequest();
        List<String> instanceIds = new ArrayList<String>();
        instanceIds.add(instanceID);
        describeRequest.setInstanceIds(instanceIds);
        DescribeInstanceStatusResult describeResult = ec2.describeInstanceStatus(describeRequest);

        while (describeResult.getInstanceStatuses().isEmpty()
                || !describeResult.getInstanceStatuses().get(0).getInstanceStatus().getStatus().equals("ok")) {
            Thread.sleep(10000);
            describeResult = ec2.describeInstanceStatus(describeRequest);
        }

        System.out.println("Instance created!");
        DescribeInstancesResult describeInstancesRequest = ec2.describeInstances();
        List<Reservation> reservations = describeInstancesRequest.getReservations();
        for (Reservation reservation : reservations) {
            for (Instance ins : reservation.getInstances()) {
                if (ins.getInstanceId().equals(instanceID)) {
                    return ins.getPublicDnsName();
                }
            }
        }
        return "";

    }


}
