package com.abb.ventyx.test.cclas.m2000.service.ccsample

import com.google.common.collect.Lists
import com.mincom.cclas.domain.sample.SampleRepository
import com.mincom.cclas.domain.schemeversion.SchemeVersionKey
import com.mincom.cclas.query.TimedEDOIFacade
import com.mincom.ellipse.edoi.ejb.ScrollableResults
import com.mincom.ellipse.edoi.ejb.ccinstrument.CCINSTRUMENTKey
import com.mincom.ellipse.edoi.ejb.ccinstrument.CCINSTRUMENTRec
import com.mincom.ellipse.edoi.ejb.ccinstrumentgroupmember.CCINSTRUMENTGROUPMEMBERRec
import com.mincom.ellipse.edoi.ejb.ccsamplescheme.CCSAMPLESCHEMEKey
import com.mincom.ellipse.edoi.ejb.ccsamplescheme.CCSAMPLESCHEMERec
import com.mincom.ellipse.edoi.ejb.ccscheme.CCSCHEMERec
import com.mincom.ellipse.hook.hooks.CoreServiceHook
import com.mincom.ellipse.types.m2000.instances.CCSampleDTO
import com.mincom.eql.Query
import com.mincom.eql.QueryFactory
import com.mincom.eql.UpdateQuery
import com.mincom.eql.impl.UpdateQueryImpl
import org.apache.commons.lang3.StringUtils

import static com.mincom.cclas.query.QueryResultsCaster.getResult
import static com.mincom.cclas.util.SpringBeanUtil.getBean
import static com.mincom.cclas.util.SystemConstants.NULL_VALUE
import static com.mincom.cclas.util.SystemConstants.YES
import static com.mincom.ellipse.types.base.util.TypeUtil.containsValue
import static java.util.Arrays.asList

public class CCSampleService_createDraft extends CoreServiceHook {
    private SampleRepository sampleRepository = getBean(SampleRepository.class);
    private TimedEDOIFacade edoiFacade = getBean(TimedEDOIFacade.class);

    @Override
    Object onPreExecute(Object inputs) {
        return null
    }

    private Set<CCSAMPLESCHEMERec> buildSourceSSRecSet(CCSampleDTO sampleDTO) {
        Query query = QueryFactory.query(CCSAMPLESCHEMERec.class);

        query.columns(Lists.newArrayList(
                CCSAMPLESCHEMERec.sampleId,
                CCSAMPLESCHEMEKey.id,
                CCSAMPLESCHEMERec.organisationCode,
                CCSAMPLESCHEMERec.laboratoryCode,
                CCSAMPLESCHEMERec.jobCode,
                CCSAMPLESCHEMERec.sampleCode,
                CCSAMPLESCHEMERec.schemeCode,
                CCSAMPLESCHEMERec.schemeLaboratoryCode,
                CCSAMPLESCHEMERec.schemeVersionNumber,
                CCSAMPLESCHEMERec.instrumentCode,
                CCSAMPLESCHEMERec.instrumentLabCode

        ));
        query.asEntity();

        query.and(CCSAMPLESCHEMERec.organisationCode.equalTo(sampleDTO.getOrganisationCode().getValue()));
        query.and(CCSAMPLESCHEMERec.laboratoryCode.equalTo(sampleDTO.getLaboratoryCode().getValue()));
        query.and(CCSAMPLESCHEMERec.sampleCode.equalTo(sampleDTO.getSampleTemplateCode().getValue()));

        Set<CCSAMPLESCHEMERec> ssSet = new HashSet<>();
        ScrollableResults scrollableResults = edoiFacade.scroll(query);
        while (scrollableResults.hasNext()) {
            CCSAMPLESCHEMERec ssRec = (CCSAMPLESCHEMERec) scrollableResults.next();
            ssSet.add(ssRec);
        }

        return ssSet;
    }

    public Set<CCSAMPLESCHEMERec> buildDestSSRecSet(CCSampleDTO sampleDTO) {
        Query query = QueryFactory.query(CCSAMPLESCHEMERec.class);

        query.columns(Lists.newArrayList(
                CCSAMPLESCHEMERec.sampleId,
                CCSAMPLESCHEMEKey.id,
                CCSAMPLESCHEMERec.organisationCode,
                CCSAMPLESCHEMERec.laboratoryCode,
                CCSAMPLESCHEMERec.jobCode,
                CCSAMPLESCHEMERec.sampleCode,
                CCSAMPLESCHEMERec.schemeCode,
                CCSAMPLESCHEMERec.schemeLaboratoryCode,
                CCSAMPLESCHEMERec.schemeVersionNumber,
                CCSAMPLESCHEMERec.instrumentCode,
                CCSAMPLESCHEMERec.instrumentLabCode
        ));
        query.asEntity();

        query.and(CCSAMPLESCHEMERec.organisationCode.equalTo(sampleDTO.getOrganisationCode().getValue()));
        query.and(CCSAMPLESCHEMERec.laboratoryCode.equalTo(sanitiseCode(sampleDTO.getLaboratoryCode().getValue())));
        query.and(CCSAMPLESCHEMERec.sampleCode.equalTo(sampleDTO.getSampleCode().getValue()));

        Set<CCSAMPLESCHEMERec> ssSet = new HashSet<>();
        ScrollableResults scrollableResults = edoiFacade.scroll(query);
        while (scrollableResults.hasNext()) {
            CCSAMPLESCHEMERec ssRec = (CCSAMPLESCHEMERec) scrollableResults.next();
            ssSet.add(ssRec);
        }

        return ssSet;
    }

    private Map<SchemeVersionKey, CCSAMPLESCHEMERec> buildSourceSVKeyAndSSRecMap(CCSampleDTO sampleDTO) {
        Map<SchemeVersionKey, CCSAMPLESCHEMERec> sourceSchemeVersionKeyAndSSRecMap = new HashMap<>();

        Set<CCSAMPLESCHEMERec> sourceSSSet = buildSourceSSRecSet(sampleDTO);
        if (sourceSSSet == null) {
            return;
        }
        for (CCSAMPLESCHEMERec sourceSSRec : sourceSSSet) {
            SchemeVersionKey schemeVersionKey = toSchemeVersionKey(sourceSSRec)

            sourceSchemeVersionKeyAndSSRecMap.putIfAbsent(schemeVersionKey, sourceSSRec);
        }

        return sourceSchemeVersionKeyAndSSRecMap;
    }

    private SchemeVersionKey toSchemeVersionKey(CCSAMPLESCHEMERec sourceSSRec) {
        SchemeVersionKey schemeVersionKey = new SchemeVersionKey(
                sourceSSRec.getOrganisationCode(),
                sanitiseCode(sourceSSRec.getSchemeLaboratoryCode()),
                sourceSSRec.getSchemeCode(),
                sourceSSRec.getSchemeVersionNumber());
        schemeVersionKey
    }


    @Override
    Object onPostExecute(Object input, Object result, Object returnWarnings) {

        CCSampleDTO sampleDTO = input as CCSampleDTO;

        if (sampleDTO == null) {
            return null;
        }

        if (!containsValue(sampleDTO.getLaboratoryCode())) {
            return result
        }

        Set<CCSAMPLESCHEMERec> destLatestSSRecSet = buildDestSSRecSet(sampleDTO);
        if (destLatestSSRecSet == null) {
            return result;
        }

        Map<SchemeVersionKey, CCSAMPLESCHEMERec> sourceSVKeyAndSourceSSRecMap = buildSourceSVKeyAndSSRecMap(sampleDTO);

        List<UpdateQuery> ssUpdateQueries = new ArrayList<>();
        for (CCSAMPLESCHEMERec latestSSRec : destLatestSSRecSet) {

            Query query = getLatestSSRecToUpdateQuery(latestSSRec)
            CCSAMPLESCHEMERec sourceSSRec = sourceSVKeyAndSourceSSRecMap.get(toSchemeVersionKey(latestSSRec));

            if (sourceSSRec == null) {
                throw new RuntimeException("Unable to locate source SSRec based on schemeVersionKey. This must be dev's bug.");
            }

            String instrumentCode = null;
            String instrumentLabCode = null;
            if (!containsValue(sourceSSRec.getInstrumentCode())) {
                String[] results = getInstrumentRecViaInstrumentGroup(sourceSSRec)

                if (results != null && results.length == 2) {
                    instrumentCode = results[0]
                    instrumentLabCode = results[1]
                }
            }

            UpdateQuery updateQuery = new UpdateQueryImpl(query);

            updateQuery.set(CCSAMPLESCHEMERec.instrumentLabCode,
                    sanitiseCode(containsValue(sourceSSRec.getInstrumentLabCode()) ? sourceSSRec.getInstrumentLabCode() : instrumentLabCode))
            updateQuery.set(CCSAMPLESCHEMERec.instrumentCode,
                    sanitiseCode(containsValue(sourceSSRec.getInstrumentCode()) ? sourceSSRec.getInstrumentCode() : instrumentCode))

            ssUpdateQueries.add(updateQuery);
        }

        if (!ssUpdateQueries.isEmpty()) {
            edoiFacade.updateAllBatch(ssUpdateQueries);
        }

        return result
    }

    private String[] getInstrumentRecViaInstrumentGroup(CCSAMPLESCHEMERec sourceSSRec) {
        Query instrumentCodeQuery = QueryFactory.query(CCINSTRUMENTRec.class);
        instrumentCodeQuery
                .columns(asList(CCINSTRUMENTRec.code, CCINSTRUMENTRec.laboratoryCode))
                .and(CCSCHEMERec.instrumentGroupId.equalTo(CCINSTRUMENTGROUPMEMBERRec.instrumentGroupId))
                .and(CCINSTRUMENTGROUPMEMBERRec.instrumentId.equalTo(CCINSTRUMENTKey.id))
                .and(CCSCHEMERec.organisationCode.equalTo(sourceSSRec.getOrganisationCode()))
                .and(CCSCHEMERec.laboratoryCode.equalTo(sourceSSRec.getLaboratoryCode()))
                .and(CCSCHEMERec.code.equalTo(sourceSSRec.getSchemeCode()))
                .and(CCINSTRUMENTRec.organisationCode.equalTo(sourceSSRec.getOrganisationCode()))
                .and(CCINSTRUMENTRec.laboratoryCode.equalTo(sourceSSRec.getLaboratoryCode()))
                .and(CCINSTRUMENTRec.isActive.equalTo(YES))
                .orderByAscending(CCINSTRUMENTGROUPMEMBERRec.aix2)
                .setMaxResults(1)

        String[] result = getResult(edoiFacade.scroll(instrumentCodeQuery));
        result
    }

    private Query getLatestSSRecToUpdateQuery(CCSAMPLESCHEMERec latestSSRec) {
        Query query = QueryFactory.query(CCSAMPLESCHEMERec.class);

        query.and(CCSAMPLESCHEMERec.organisationCode.equalTo(latestSSRec.getOrganisationCode()));
        query.and(CCSAMPLESCHEMERec.laboratoryCode.equalTo(sanitiseCode(latestSSRec.getLaboratoryCode())));
        query.and(CCSAMPLESCHEMERec.jobCode.equalTo(latestSSRec.getJobCode()));
        query.and(CCSAMPLESCHEMERec.sampleCode.equalTo(latestSSRec.getSampleCode()));
        query.and(CCSAMPLESCHEMERec.schemeLaboratoryCode.equalTo(sanitiseCode(latestSSRec.getSchemeLaboratoryCode())));
        query.and(CCSAMPLESCHEMERec.schemeCode.equalTo(latestSSRec.getSchemeCode()));
        query.and(CCSAMPLESCHEMERec.schemeVersionNumber.equalTo(latestSSRec.getSchemeVersionNumber()));
        query
    }

    private String sanitiseCode(String value) {
        if (StringUtils.isBlank(value)) {
            return NULL_VALUE;
        } else {
            return value;
        }
    }
}