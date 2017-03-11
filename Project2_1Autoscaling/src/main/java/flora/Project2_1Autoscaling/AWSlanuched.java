package flora.Project2_1Autoscaling;

import java.util.ArrayList;
import java.util.List;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.Tag;

public class AWSlanuched {
	static AmazonEC2 ec2 = null;

	public static String[] Lannchvm(String[] args) {
		String lg = null;
		List<String> instancesid = new ArrayList<String>();
		int flag = 0;
		String imageid = args[0];
		String type = args[1];
		String security = args[2];
		AWSCredentials credentials = null;
		try {
			credentials = new ProfileCredentialsProvider().getCredentials();
		} catch (Exception e) {
			e.printStackTrace();
		}

		// Create the AmazonEC2Client object so we can call various APIs.
		ec2 = new AmazonEC2Client(credentials);
		Region usEast1 = Region.getRegion(Regions.US_EAST_1);
		ec2.setRegion(usEast1);

		try {
			RunInstancesRequest runInstancesRequest = new RunInstancesRequest();
			runInstancesRequest.withImageId(imageid).withInstanceType(type).withMinCount(1).withMaxCount(1)
					.withKeyName("Arsenal").withSecurityGroups(security).withMonitoring(true);
			// System.out.println("setted");
			RunInstancesResult lgresult = ec2.runInstances(runInstancesRequest);
			// System.out.println(lgresult);
			List<Instance> instances = lgresult.getReservation().getInstances();

			for (com.amazonaws.services.ec2.model.Instance instance : instances) {
				CreateTagsRequest createTagsRequest = new CreateTagsRequest();
				createTagsRequest.withResources(instance.getInstanceId()).withTags(new Tag("Project", "2.1"));
				ec2.createTags(createTagsRequest);
				instancesid.add(instance.getInstanceId());
			}
			while (lg == null || lg.equals("")) {

				DescribeInstancesRequest describelg = new DescribeInstancesRequest();
				describelg.setInstanceIds(instancesid);
				try {
					DescribeInstancesResult describeresult = ec2.describeInstances(describelg);
					List<Reservation> describeinstances = describeresult.getReservations();
					lg = describeinstances.get(0).getInstances().get(0).getPublicDnsName();
					System.out.println(describeresult.toString());
					System.out.println(lg);
				} catch (Exception e) {
					// e.printStackTrace();
					Thread.sleep(1000);
					continue;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		String[] forreturn = { lg, instancesid.get(0) };
		return forreturn;
	}

}
