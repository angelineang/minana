/**
 * Created by MYANANG on 4/26/2019.
 */

import static com.mincom.base.InvalidArgumentException.validateNotNull;

public class SampleRecKey {
    private final String organisationCode;
    private final String laboratoryCode;
    private final String jobCode;
    private final String sampleCode;
    private final String sampleId;

    public SampleRecKey(String organisationCode, String laboratoryCode, String jobCode, String sampleCode, String sampleId) {
        validateNotNull(organisationCode, "organisationCode");
        validateNotNull(laboratoryCode, "laboratoryCode");
        validateNotNull(jobCode, "jobCode");
        validateNotNull(sampleCode, "sampleCode");
        this.organisationCode = organisationCode;
        this.laboratoryCode = laboratoryCode;
        this.jobCode = jobCode;
        this.sampleCode = sampleCode;
        this.sampleId = sampleId;
    }

    public String getOrganisationCode() {
        return organisationCode;
    }

    public String getLaboratoryCode() {
        return laboratoryCode;
    }

    public String getJobCode() {
        return jobCode;
    }

    public String getSampleCode() {
        return sampleCode;
    }

    public String getSampleId() {
        return sampleId;
    }
/*
    @Override
    public int hashCode() {
        int result = schemeCode.hashCode();
        result = 31 * result + analyteCode.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "SchemeAnalyteKey{" +
                "schemeCode='" + schemeCode + '\'' +
                ", analyteCode='" + analyteCode + '\'' +
                '}';
    }*/
}
