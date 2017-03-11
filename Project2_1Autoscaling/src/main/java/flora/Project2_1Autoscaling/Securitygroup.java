package flora.Project2_1Autoscaling;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.CreateSecurityGroupRequest;
import com.amazonaws.services.ec2.model.CreateSecurityGroupResult;
import com.amazonaws.services.ec2.model.DeleteSecurityGroupRequest;
import com.amazonaws.services.ec2.model.DeleteSecurityGroupResult;
import com.amazonaws.services.ec2.model.IpPermission;

public class Securitygroup {
	static CreateSecurityGroupResult result=null;
	static String id=null;
	
	public static String CreateSecurityGroup(String groupname) {
		int flag=0;
		AWSCredentials credentials = null;
		try {
			credentials = new ProfileCredentialsProvider().getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
					+ "Please make sure that your credentials file is at the correct "
					+ "location (~/.aws/credentials), and is in valid format.", e);
		}
		AmazonEC2 ec2 = new AmazonEC2Client(credentials);
		Region usEast1 = Region.getRegion(Regions.US_EAST_1);
		ec2.setRegion(usEast1);

		// Create a new security group.
		try {
			CreateSecurityGroupRequest securityGroupRequest = new CreateSecurityGroupRequest(groupname, "Project2_1");
			result = ec2.createSecurityGroup(securityGroupRequest);
			System.out.println(String.format("Security group created: [%s]", result.getGroupId()));
		} catch (AmazonServiceException ase) {
			// Likely this means that the group is already created, so ignore.
			System.out.println(ase.getMessage());
		}
		IpPermission ipPermission1 = new IpPermission().withIpProtocol("tcp").withFromPort(new Integer(80))
				.withToPort(new Integer(80)).withIpRanges("0.0.0.0/0");
		IpPermission ipPermission2 = new IpPermission().withIpProtocol("tcp").withFromPort(new Integer(22))
				.withToPort(new Integer(22)).withIpRanges("0.0.0.0/0");
		IpPermission ipPermission3 = new IpPermission().withIpProtocol("tcp").withFromPort(new Integer(15619))
				.withToPort(new Integer(15619)).withIpRanges("0.0.0.0/0");
		IpPermission ipPermission4 = new IpPermission().withIpProtocol("tcp").withFromPort(new Integer(15319))
				.withToPort(new Integer(15319)).withIpRanges("0.0.0.0/0");
		List<IpPermission> ipPermissions = new ArrayList<IpPermission>();
		ipPermissions.add(ipPermission1);
		ipPermissions.add(ipPermission2);
		ipPermissions.add(ipPermission3);
		ipPermissions.add(ipPermission4);
		while(flag==0){
		try {
			
			// Authorize the ports to the used.
			AuthorizeSecurityGroupIngressRequest ingressRequest = new AuthorizeSecurityGroupIngressRequest(
					groupname, ipPermissions);
			ec2.authorizeSecurityGroupIngress(ingressRequest);flag=1;
			System.out.println(String.format("Ingress port authroized: [%s]", ipPermissions.toString()));
			
		} catch (AmazonServiceException ase) {
			// Ignore because this likely means the zone has already been
			// authorized.
			//System.out.println(ase.getMessage());
			}
		}
		return result.getGroupId();
		
	}
    public void deleteSecurityGroup(String groupname) {
    	AWSCredentials credentials = null;
		try {
			credentials = new ProfileCredentialsProvider().getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
					+ "Please make sure that your credentials file is at the correct "
					+ "location (~/.aws/credentials), and is in valid format.", e);
		}
		AmazonEC2 ec2 = new AmazonEC2Client(credentials);
		Region usEast1 = Region.getRegion(Regions.US_EAST_1);
		ec2.setRegion(usEast1);
		try{
    	DeleteSecurityGroupRequest deleteSecurityGroupRequest=new DeleteSecurityGroupRequest(groupname);
    	DeleteSecurityGroupResult result=ec2.deleteSecurityGroup(deleteSecurityGroupRequest);
    	System.out.println(String.format("Security group deleted: [%s]", result.toString()));
		}catch (Exception e) {
			// TODO: handle exception
		}
	}
}
