package lxfree.query1.autoscaling;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.CreateSecurityGroupRequest;
import com.amazonaws.services.ec2.model.CreateSecurityGroupResult;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusResult;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.IpPermission;
import com.amazonaws.services.ec2.model.IpRange;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.Tag;

public class UndertowStartUp {
	
	protected static String KEY_NAME = "Project1_1";
	protected static String INSTANCE_TYPE = "m3.medium";
	protected static String SECURITY_GROUP = "rxl-project21";
	protected static AmazonEC2 ec2;
	private static String LG_IMAGE = "ami-dfc835c9";
	private static String DC_IMAGE = "ami-2fca3739";

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
		ec2 = AmazonEC2ClientBuilder.standard().withRegion(Regions.US_EAST_1)
				.withCredentials(new AWSStaticCredentialsProvider(credentials)).build();

		// Create a new security group.
		try {
			CreateSecurityGroupRequest csgr = new CreateSecurityGroupRequest();
			csgr.withGroupName("rxl-project21").withDescription("My security group");
			CreateSecurityGroupResult createSecurityGroupResult = ec2.createSecurityGroup(csgr);

			IpPermission ipPermission = new IpPermission();

			List<IpRange> ipList = new ArrayList<IpRange>();
			IpRange range = new IpRange();
			range.setCidrIp("0.0.0.0/0");
			ipList.add(range);

			ipPermission.withIpv4Ranges(ipList).withIpProtocol("tcp").withFromPort(80).withToPort(80);
			AuthorizeSecurityGroupIngressRequest authorizeSecurityGroupIngressRequest = new AuthorizeSecurityGroupIngressRequest();

			authorizeSecurityGroupIngressRequest.withGroupName("rxl-project21").withIpPermissions(ipPermission);
			ec2.authorizeSecurityGroupIngress(authorizeSecurityGroupIngressRequest);

			System.out.println("Security group created");
			
			String lgdnsName = launchInstance(LG_IMAGE);
			System.out.println("Load Generator DNS:" + lgdnsName);
			String dcdnsName = launchInstance(DC_IMAGE);
			System.out.println("Data Center DNS:" + lgdnsName);
			//Authorize load generator
			String authURL = "http://" + lgdnsName + "/password?passwd=" + args[1] + "&andrewid=" + args[0];
			String authResponse = sendRequest(authURL);			
	        String launchTest = "http://" + lgdnsName + "/test/horizontal?dns=" + dcdnsName;
	        String responseTest = sendRequest(launchTest);
	        String testlog = responseTest.substring(responseTest.indexOf("test"), responseTest.indexOf(".log")+4);
	        AutoScaling.autoScaling(lgdnsName, testlog);
			
		} catch (AmazonServiceException ase) {
			// Likely this means that the group is already created, so ignore.
			System.out.println(ase.getMessage());
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static String launchInstance(String imageID) throws InterruptedException {
		//Launch instance
		RunInstancesRequest runInstancesRequest =
				   new RunInstancesRequest();

		runInstancesRequest.withImageId(imageID)
		                   .withInstanceType(INSTANCE_TYPE)
		                   .withMinCount(1)
		                   .withMaxCount(1)
		                   .withKeyName(KEY_NAME)
		                   .withSecurityGroups(SECURITY_GROUP);
		
		RunInstancesResult result = ec2.runInstances(runInstancesRequest);
		
		//Tag instance
		Instance instance = result.getReservation().getInstances().get(0);
		String instanceID = instance.getInstanceId();
		CreateTagsRequest createTagsRequest = new CreateTagsRequest();
		createTagsRequest.withResources(instanceID).withTags(new Tag("Project", "2.1"));
		ec2.createTags(createTagsRequest);
		
		//Check instance status
		DescribeInstanceStatusRequest describeRequest = new DescribeInstanceStatusRequest();
		List<String> instanceIds = new ArrayList<String>();
		instanceIds.add(instanceID);
		describeRequest.setInstanceIds(instanceIds);
		DescribeInstanceStatusResult  describeResult = ec2.describeInstanceStatus(describeRequest);
		
		while(describeResult.getInstanceStatuses().isEmpty() || 
				!describeResult.getInstanceStatuses().get(0).getInstanceStatus().getStatus().equals("ok")) {
			Thread.sleep(10000);
			describeResult = ec2.describeInstanceStatus(describeRequest);
		}

		System.out.println("Instance created!");
	    DescribeInstancesResult describeInstancesRequest = ec2.describeInstances();
	    List<Reservation> reservations = describeInstancesRequest.getReservations();
	    for(Reservation reservation: reservations) {
	    	for(Instance ins: reservation.getInstances()){
	    		if(ins.getInstanceId().equals(instanceID)) {
	    			return ins.getPublicDnsName();
	    		}
	    	}
	    }
		return "";
		
	}
	
    private static String sendRequest(String urlString) throws MalformedURLException, InterruptedException {
        String response = "";
        URL url = new URL(urlString);
        int code = 0;
        HttpURLConnection connection = null;
			while(code != 200) {
				try {
					connection = (HttpURLConnection) url.openConnection();
					code = connection.getResponseCode();
					if(code != 200) {
						Thread.sleep(10000);						
					}
				} catch (IOException e) {
					continue;
				}
			}

		BufferedReader in = null;
		try {
         in = new BufferedReader(new InputStreamReader(connection.getInputStream(), "UTF-8"));
        String str;
			while ((str = in.readLine()) != null) {
			    response += str;
			}
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
        

        return response;
    }
}
