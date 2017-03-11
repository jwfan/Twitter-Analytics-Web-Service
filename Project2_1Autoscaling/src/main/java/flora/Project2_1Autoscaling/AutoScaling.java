package flora.Project2_1Autoscaling;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Set;
import java.util.Map.Entry;
import org.ini4j.Ini;
import org.ini4j.Profile.Section;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.autoscaling.AmazonAutoScaling;
import com.amazonaws.services.autoscaling.AmazonAutoScalingClient;
import com.amazonaws.services.autoscaling.model.DeleteAutoScalingGroupRequest;
import com.amazonaws.services.autoscaling.model.DeleteAutoScalingGroupResult;
import com.amazonaws.services.autoscaling.model.DeleteLaunchConfigurationRequest;
import com.amazonaws.services.autoscaling.model.DeleteLaunchConfigurationResult;
import com.amazonaws.services.autoscaling.model.UpdateAutoScalingGroupRequest;
import com.amazonaws.services.autoscaling.model.UpdateAutoScalingGroupResult;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DeleteSecurityGroupRequest;
import com.amazonaws.services.ec2.model.DeleteSecurityGroupResult;
import com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancingClient;
import com.amazonaws.services.elasticloadbalancing.model.DeleteLoadBalancerRequest;
import com.amazonaws.services.elasticloadbalancing.model.DeleteLoadBalancerResult;

public class AutoScaling {
	static String[] lgdns = null;
	static String elbdns = null;
	// static String[] dcdns2 = null;
	static String subpass = null;
	static String andrewid = null;
	static String testId = null;
	static String secid =null;
	public static void main(String[] args) {
		String[] lg = new String[] { "ami-dfc835c9", "m3.medium", "lgsecuritygroup" };
		// String[] dc1 = new String[] { "ami-2fca3739", "m3.medium",
		// "commonsecuritygroup" };
		// String[] dc2 = new String[] { "ami-2fca3739", "m3.large",
		// "commonsecuritygroup" };
		subpass = System.getenv("subpass");
		andrewid = System.getenv("andrewid");

		Securitygroup.CreateSecurityGroup("lgsecuritygroup");
		secid = Securitygroup.CreateSecurityGroup("commonsecuritygroup");

		lgdns = AWSlanuched.Lannchvm(lg);
		// dcdns1 = AWSlanuched.Lannchvm(dc1);

		System.out.println(lgdns[0] + lgdns[1]);
		// System.out.println(dcdns1[0] + dcdns1[1]);

		// String[] originaldc=new String[]{dcdns1[1],dcdns1[2]};

		elbdns = ELB.createdELB(lgdns[0], secid);
		AutoscalingGroup.Launchconfig(secid, "m3.medium", "my-launch-config");
		AutoscalingGroup.Asgroup();
		//lgdns[0]="ec2-184-72-204-3.compute-1.amazonaws.com";
		//elbdns="my-elb-377090009.us-east-1.elb.amazonaws.com";
		httpclient();
		
		//AutoscalingGroup.modifyAS(secid, "m3.large", "my-modified-config");
		monitorRes();
		deleteRes();
	}

	public static void httpclient() {
		URL url1 = null;
		String warmId=null;
		Ini ini = new Ini();
		int flag1 = 0;
		int flag2 = 0;
		int flag22 = 0;
		int flag3 = 0;
		HttpURLConnection connection = null;
		BufferedReader input = null;
		String s = null;
		while (flag1 == 0) {
			try {

				url1 = new URL("http://" + lgdns[0] + "/password?passwd=" + subpass + "&andrewid=" + andrewid);
				connection = (HttpURLConnection) url1.openConnection();
				connection.setRequestMethod("GET");
				connection.setRequestProperty("Host", lgdns[0]);
				connection.setRequestProperty("Charset", "UTF-8");
				connection.setRequestProperty("accept", "*/*");
				connection.connect();
				String outputs = "";
				input = new BufferedReader(new InputStreamReader(connection.getInputStream()));
				while ((s = input.readLine()) != null) {
					outputs += s;
				}
				System.out.println(outputs);
				input.close();
				flag1 = 1;

			} catch (IOException e) {
				// e.printStackTrace();
			}

		}

		System.out.println("Conected a");
		while (flag2 == 0) {
			try {

				url1 = new URL("http://" + lgdns[0] + "/warmup?dns=" + elbdns);
				connection = (HttpURLConnection) url1.openConnection();
				connection.setRequestMethod("GET");
				connection.setRequestProperty("Host", lgdns[0] );
				connection.setRequestProperty("Charset", "UTF-8");
				connection.setRequestProperty("accept", "*/*");

				connection.connect();
				input = new BufferedReader(new InputStreamReader(connection.getInputStream()));
				while ((s = input.readLine()) != null) {
					warmId += s;
					// System.out.println(testId);
				}
				System.out.println(warmId);
				int startIndex = warmId.indexOf("name=test.") + 10;
				int lastIndex = warmId.indexOf(".log'");
				warmId = warmId.substring(startIndex, lastIndex);
				System.out.println("warmId:" + warmId);
				input.close();
				flag2=1;
			} catch (IOException e) {
				// e.printStackTrace();
			}
		}
		while (flag22 == 0) {
			try {

				url1 = new URL("http://" + lgdns[0] + "/log?name=test." + warmId+".log");
				connection = (HttpURLConnection) url1.openConnection();
				connection.setRequestMethod("GET");
				connection.setRequestProperty("Host", lgdns[0]);
				connection.setRequestProperty("Charset", "UTF-8");
				connection.setRequestProperty("accept", "*/*");
				connection.connect();
				ini.load(connection.getInputStream());
				if (!ini.isEmpty()) {
					System.out.println("readed warm log");
					Set<Entry<String, Section>> set = ini.entrySet();
					for (Entry<String, Section> entry : set) {
						String secName = entry.getKey();
						System.out.println(secName);
						if (secName.contains("Minute")) {
							int x = Integer.parseInt(secName.substring((secName.indexOf("Minute") + 7)));
							System.out.println(x);
							if (x == 15) {
								System.out.println("warmed up");
								flag22 = 1;
							}
						}
					}
				}
			} catch (IOException e) {
				// e.printStackTrace();
			}
		}
		while (flag3 == 0) {
			try {
				url1 = new URL("http://" + lgdns[0] + "/autoscaling?dns=" + elbdns);
				connection = (HttpURLConnection) url1.openConnection();
				connection.setRequestMethod("GET");
				connection.setRequestProperty("Host", lgdns[0]);
				connection.setRequestProperty("Charset", "UTF-8");
				connection.setRequestProperty("accept", "*/*");
				connection.connect();
				input = new BufferedReader(new InputStreamReader(connection.getInputStream()));
				while ((s = input.readLine()) != null) {
					testId += s;
					// System.out.println(testId);
				}
				System.out.println(testId);
				int startIndex = testId.indexOf("name=test.") + 10;
				int lastIndex = testId.indexOf(".log'");
				testId = testId.substring(startIndex, lastIndex);
				System.out.println("testId:" + testId);
				input.close();
				flag3 = 1;
			} catch (IOException e) {
				// e.printStackTrace();
			}
		}
	}

	public static void monitorRes() {
		URL url1 = null;
		double minute = 0;
		HttpURLConnection connection = null;
		Ini ini = new Ini();
		while (minute<48) {
					try {
						// String s = null;
						// String logfile = null;
						url1 = new URL("http://" + lgdns[0] + "/log?name=test." + testId + ".log");
						connection = (HttpURLConnection) url1.openConnection();
						connection.setRequestMethod("GET");
						connection.setRequestProperty("Charset", "UTF-8");
						connection.setRequestProperty("Host", lgdns[0] );
						connection.setRequestProperty("accept", "*/*");
						connection.connect();
						ini.load(connection.getInputStream());
						if (!ini.isEmpty()) {
							System.out.println("readed");
							Set<Entry<String, Section>> set = ini.entrySet();
							for (Entry<String, Section> entry : set) {
								String secName = entry.getKey();
								System.out.println(secName);
								if (secName.contains("Minute")) {
									int x = Integer.parseInt(secName.substring((secName.indexOf("Minute") + 7)));
									if(x>minute){
										minute = x;
								}
							}
						}
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}

	public static void deleteRes(){
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
		AmazonAutoScaling asclient = new AmazonAutoScalingClient(credentials);
		AmazonElasticLoadBalancingClient balancerclient = new AmazonElasticLoadBalancingClient();
		UpdateAutoScalingGroupRequest updateRequest =new UpdateAutoScalingGroupRequest().withAutoScalingGroupName("my-auto-scaling-group").withMinSize(0).withMaxSize(0).withDesiredCapacity(0);
		UpdateAutoScalingGroupResult updateResult=asclient.updateAutoScalingGroup(updateRequest);
		System.out.println(updateRequest.toString());
		
		AutoscalingGroup.describeins();
		/*
		TerminateInstancesRequest deletein=new TerminateInstancesRequest().withInstanceIds("");
		TerminateInstancesResult resultin=ec2.terminateInstances(deletein);
		System.out.println(resultin.toString());*/
		
		DeleteLoadBalancerRequest deleteelb=new DeleteLoadBalancerRequest().withLoadBalancerName("my-elb");
		DeleteLoadBalancerResult elbresult=balancerclient.deleteLoadBalancer(deleteelb);
		System.out.println(elbresult.toString());
		
		DeleteAutoScalingGroupRequest deleteas=new DeleteAutoScalingGroupRequest().withAutoScalingGroupName("my-auto-scaling-group").withForceDelete(true);
		DeleteAutoScalingGroupResult asResult=asclient.deleteAutoScalingGroup(deleteas);
		System.out.println(asResult.toString());
		
		DeleteLaunchConfigurationRequest deletelc=new DeleteLaunchConfigurationRequest().withLaunchConfigurationName("my-launch-config");
		DeleteLaunchConfigurationResult cfresult=asclient.deleteLaunchConfiguration(deletelc);
		//DeleteLaunchConfigurationRequest deletelc1=new DeleteLaunchConfigurationRequest().withLaunchConfigurationName("my-modified-config");
		//DeleteLaunchConfigurationResult cfresult1=asclient.deleteLaunchConfiguration(deletelc1);
		System.out.println(cfresult.toString());
		while (flag==0)
		try{
		DeleteSecurityGroupRequest deletesg=new DeleteSecurityGroupRequest().withGroupId(secid);
		DeleteSecurityGroupResult sgresult=ec2.deleteSecurityGroup(deletesg);flag=1;
		System.out.println(sgresult.toString());
		}catch (Exception e) {
			// TODO: handle exception
		}
		
	}
}
