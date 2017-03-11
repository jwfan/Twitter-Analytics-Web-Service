package flora.Project2_1Autoscaling;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.*;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.elasticloadbalancing.*;
import com.amazonaws.services.elasticloadbalancing.model.ConfigureHealthCheckRequest;
import com.amazonaws.services.elasticloadbalancing.model.ConfigureHealthCheckResult;
import com.amazonaws.services.elasticloadbalancing.model.CreateLoadBalancerRequest;
import com.amazonaws.services.elasticloadbalancing.model.CreateLoadBalancerResult;
import com.amazonaws.services.elasticloadbalancing.model.DescribeLoadBalancersRequest;
import com.amazonaws.services.elasticloadbalancing.model.DescribeLoadBalancersResult;
import com.amazonaws.services.elasticloadbalancing.model.HealthCheck;
import com.amazonaws.services.elasticloadbalancing.model.Listener;

public class ELB {
	static AmazonElasticLoadBalancing elb = null;
	static AmazonEC2 ec2 = null;
	static String elbdns = null;

	public static String createdELB(String lgdns, String secgroup) {
		AWSCredentials credentials = null;

		List<String> subnets = new ArrayList<String>();
		subnets.add("subnet-7297025f");
		subnets.add("subnet-4e2b6f07");
		try {
			credentials = new ProfileCredentialsProvider().getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
					+ "Please make sure that your credentials file is at the correct "
					+ "location (~/.aws/credentials), and is in valid format.", e);
		}
		try {
			elb = AmazonElasticLoadBalancingClientBuilder.standard()
					.withCredentials(new AWSStaticCredentialsProvider(credentials)).build();
			// elb=new AmazonElasticLoadBalancingClient(credentials);
			System.out.println("elb credential");
		} catch (Exception e) {
			e.printStackTrace();
		}
		Region usEast1 = Region.getRegion(Regions.US_EAST_1);
		com.amazonaws.services.elasticloadbalancing.model.Tag tag = new com.amazonaws.services.elasticloadbalancing.model.Tag();
		tag.setKey("Project");
		tag.setValue("2.1");
		// elb.setRegion(usEast1);
		try {
			//com.amazonaws.services.elasticloadbalancing.model.Instance[] dcc = new com.amazonaws.services.elasticloadbalancing.model.Instance[id.length];
			Listener listener = new Listener().withProtocol("HTTP").withInstancePort(80).withLoadBalancerPort(80);
			AmazonElasticLoadBalancingClient balancerclient = new AmazonElasticLoadBalancingClient();
			CreateLoadBalancerRequest request = new CreateLoadBalancerRequest().withLoadBalancerName("my-elb")
					.withSubnets("subnet-2af88e71", "subnet-c49ee6e9").withSecurityGroups(secgroup).withTags(tag)
					.withListeners(listener);
			CreateLoadBalancerResult result = balancerclient.createLoadBalancer(request);
			HealthCheck healthCheck = new HealthCheck().withTarget("HTTP:80/heartbeat?lg=" + lgdns).withTimeout(5)
					.withInterval(30).withHealthyThreshold(10).withUnhealthyThreshold(2);
			ConfigureHealthCheckRequest healthrequest = new ConfigureHealthCheckRequest("my-elb", healthCheck);
			ConfigureHealthCheckResult healthresult = balancerclient.configureHealthCheck(healthrequest);
			DescribeLoadBalancersRequest des=new DescribeLoadBalancersRequest().withLoadBalancerNames("my-elb");
			DescribeLoadBalancersResult desresult=balancerclient.describeLoadBalancers(des);
			String temp=desresult.toString();
			elbdns=temp.substring(temp.indexOf("DNSName:")+9,temp.indexOf("CanonicalHostedZoneName")-1);
			System.out.println(elbdns);
			/*for (int i = 0; i < id.length; i++) {
				dcc[i] = new com.amazonaws.services.elasticloadbalancing.model.Instance(id[i]);
			}*/
			
			ec2 = new AmazonEC2Client(credentials);
		//Region usEast1 = Region.getRegion(Regions.US_EAST_1);

		
		} catch (Exception e) {
			e.printStackTrace();
		}
		return elbdns;
	}

	public static void deleteELB() {

	}
}
