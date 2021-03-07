
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.regions.Regions;

public class Main {
    //constants
    public static final Regions REGION = Regions.US_EAST_1;
    private static final String ONE_GRAM_ENG = "s3://datasets.elasticmapreduce/"
            + "ngrams/books/20090715/eng-us-all/1gram/data";
    private static final String TWO_GRAM_ENG = "s3://datasets.elasticmapreduce/"
            + "ngrams/books/20090715/eng-us-all/2gram/data";
    private static final String ONE_GRAM_HEB = "s3://datasets.elasticmapreduce/"
            + "ngrams/books/20090715/heb-all/1gram/data";
    private static final String TWO_GRAM_HEB = "s3://datasets.elasticmapreduce/"
            + "ngrams/books/20090715/heb-all/2gram/data";
    private static final String BUCKET_URL = "s3n://nitay-omer-assignment2/";
    private static final String INSTANCE_TYPE = InstanceType.M4Large.toString();
    private static final String JOB1_JAR_URL = BUCKET_URL + "jars/job1_without.jar";
    private static final String JOB2_JAR_URL = BUCKET_URL + "jars/job2.jar";
    private static final String JOB3_JAR_URL = BUCKET_URL + "jars/job3.jar";
    private static final String JOB4_JAR_URL = BUCKET_URL + "jars/job4.jar";
    private static final String LOGS_FOLDER_NAME = BUCKET_URL + "logs/";
    private static final String JOB1_OUTPUT = BUCKET_URL + "output/output1";
    private static final String JOB2_OUTPUT = BUCKET_URL + "output/output2";
    private static final String JOB3_OUTPUT = BUCKET_URL + "output/output3";
    private static final String JOB4_OUTPUT = BUCKET_URL + "output/output4";

    public static void main(String[] args) {
        if(args.length == 0)
            throw new NullPointerException();
        boolean isHeb = args[0].toLowerCase().equals("heb");
        String input1, input2;
        if(isHeb)
        {
            System.out.println("Set language to be Heb");
            input1 = ONE_GRAM_HEB;
            input2 = TWO_GRAM_HEB;
        }
        else
        {
            System.out.println("Set language to be Eng");
            input1 = ONE_GRAM_ENG;
            input2 = TWO_GRAM_ENG;
        }
        //------------------------------------------Connection------------------------------------------
        System.out.println("Trying To Connect...");
        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder
                .standard()
                .withRegion(REGION)
                .withCredentials(new ProfileCredentialsProvider())
                .build();
        System.out.println("Connected");
        System.out.println("Configuring Steps");
        //------------------------------------------JOB1------------------------------------------
        HadoopJarStepConfig step1_conf = new HadoopJarStepConfig()
                .withJar(JOB1_JAR_URL)
                .withArgs(input1, input2, JOB1_OUTPUT, args[0].toLowerCase());

        StepConfig step1 = new StepConfig()
                .withName("Job1")
                .withHadoopJarStep(step1_conf)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
        //------------------------------------------JOB2------------------------------------------
        HadoopJarStepConfig step2_config = new HadoopJarStepConfig()
                .withJar(JOB2_JAR_URL)
                .withArgs(JOB1_OUTPUT, JOB2_OUTPUT);

        StepConfig step2 = new StepConfig()
                .withName("Job2")
                .withHadoopJarStep(step2_config)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
        //------------------------------------------JOB3------------------------------------------
        HadoopJarStepConfig step3_config = new HadoopJarStepConfig()
                .withJar(JOB3_JAR_URL)
                .withArgs(JOB2_OUTPUT, JOB3_OUTPUT);

        StepConfig step3 = new StepConfig()
                .withName("Job3")
                .withHadoopJarStep(step3_config)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
        //------------------------------------------JOB4------------------------------------------
        HadoopJarStepConfig step4_config = new HadoopJarStepConfig()
                .withJar(JOB4_JAR_URL)
                .withArgs(JOB3_OUTPUT, JOB4_OUTPUT);

        StepConfig step4 = new StepConfig()
                .withName("Job4")
                .withHadoopJarStep(step4_config)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
        //------------------------------------------FLOW------------------------------------------
        System.out.println("Finished Configuring Steps, Trying to configure run config");
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(15)
                .withMasterInstanceType(INSTANCE_TYPE)
                .withSlaveInstanceType(INSTANCE_TYPE)
                .withHadoopVersion("2.6.0")
                .withEc2KeyName("jessica")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        // Create a flow request including all the steps
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("eng-without-combiner")
                .withInstances(instances)
                .withSteps(step1, step2, step3, step4)
                .withLogUri(LOGS_FOLDER_NAME)
                .withServiceRole("EMRDefaultRole")
                .withJobFlowRole("EMREC2Role")
                .withReleaseLabel("emr-5.20.0");

        System.out.println("Flow created, Trying to Run");

        // Run the flow
        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);

    }
}
