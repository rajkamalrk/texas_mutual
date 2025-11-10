import AWS from 'aws-sdk';
import { create } from 'xmlbuilder2';
import { initDbPool } from './db-config.mjs';

let connection;
const setupConnection = async () => {
    console.log(connection, 'connection');

    if (!connection) {
        const pool = await initDbPool();
        connection = await pool.getConnection();
        console.log("DB connection initialized.");
    }
};

const s3 = new AWS.S3();
const bucketName = process.env.S3_BucketName;

export const handler = async (event) => {
    try {
        const now = new Date();
        const timestamp = `${(now.getMonth() + 1).toString().padStart(2, '0')}` + // MM (month starts from 0)
            `${now.getDate().toString().padStart(2, '0')}` +        // DD
            `${now.getFullYear()}` +                                // YYYY
            `${now.getHours().toString().padStart(2, '0')}` +       // HH
            `${now.getMinutes().toString().padStart(2, '0')}` +     // MM
            `${now.getSeconds().toString().padStart(2, '0')}`;      // SS

        console.log('timestamp', timestamp);
        const currentTimeStamp = formateTime(new Date().toISOString());
        console.log(currentTimeStamp,'currentTimeStamp.....')        


        await setupConnection();
        console.log('event xml generator', event);
        // const data = JSON.parse(event.body);
        const tp = event.tp;
        const invoiceHeaderId = event.invoiceHeaderId;
        const billId = event.billId;
        const SERVICE_INFO_LIMIT = 6;
        let xml = '';
        let formattedXML = '';
        const params = {};
        console.log('tp', tp);
        console.log('invoiceHeaderId', invoiceHeaderId);
        const [res] = await connection.query(
            `SELECT txm_invoice_number from ${process.env.DB_2}.bill_header WHERE bill_id = ?`, [billId]
          );
        const invoiceNumber = res[0].txm_invoice_number;
        console.log('invoiceNumber....', invoiceNumber);
        const currentDate = new Date().toLocaleDateString('en-US', {
            timeZone: 'America/Chicago', // Adjust the timezone if needed
            month: '2-digit',
            day: '2-digit',
            year: 'numeric',
        });
        switch (tp) {
            case 'align':
                let alignQuery1 = `SELECT * FROM ${process.env.TABLE_1} WHERE invoice_header_id = ?`;
                let alignQuery2 = `SELECT * FROM ${process.env.TABLE_2} WHERE invoice_header_id = ?`;
                let alignQuery3 = `SELECT * FROM ${process.env.TABLE_3} WHERE invoice_header_id = ? ORDER BY CAST(line_number AS DECIMAL(10, 2)) ASC`;
                const [alignInvoiceHeaderResult] = await connection.execute(alignQuery1, [invoiceHeaderId]);
                const [alignAttachmentResult] = await connection.execute(alignQuery2, [invoiceHeaderId]);
                const [alignInvoiceDetailResult] = await connection.execute(alignQuery3, [invoiceHeaderId]);
                console.log('alignInvoiceHeaderResult', alignInvoiceHeaderResult);
                console.log('alignAttachmentResult', alignAttachmentResult);
                console.log('alignInvoiceDetailResult', alignInvoiceDetailResult);
                if (alignInvoiceHeaderResult.length === 0 || alignInvoiceDetailResult.length === 0) {
                    console.log('No records found');
                    return JSON.stringify({
                        status: 400,
                        message: "No records for xml generation",
                        errorType: "No records for xml generation"
                    });
                }
                const totalItems = Math.max(alignInvoiceDetailResult.length, 1);
                const alignInvoiceHeader = alignInvoiceHeaderResult[0];
                // const alignAttachment = alignAttachmentResult[0] || '';
                const alignInvoiceDetail = alignInvoiceDetailResult[0];
                console.log('alignInvoiceHeader', alignInvoiceHeader);
                // console.log('alignAttachment', alignAttachment);
                console.log('alignInvoiceDetail', alignInvoiceDetail);
                console.log('claim_number', alignInvoiceHeader.claim_number);
                console.log(alignInvoiceDetailResult.length,'alignInvoiceDetailResult.lenght().....');

                xml = await create({ version: '1.0', encoding: 'UTF-8' })
                    .ele('ns7:Document', {
                        'xmlns:ns6': 'http://tmi.legacy.model.header.recipientmodel',
                        'xmlns:ns5': 'http://tmi.legacy.model.header.documentmetadatamodel',
                        'xmlns:ns7': 'http://tmi.legacy.model.documentmodel',
                        'xmlns:ns2': 'http://tmi.legacy.model.header.distributionchannelmodel',
                        'xmlns:ns4': 'http://tmi.legacy.model.header.documentheadermodel',
                        'xmlns:ns3': 'http://tmi.legacy.model.header.distributiondetailsmodel'
                    })
                        .ele('ns7:BusinessObjectType').txt('AdobeXD').up()
                        .ele('ns7:DocumentEffectiveDate').txt(currentTimeStamp).up()
                        const documentHeader = xml.ele('ns7:DocumentHeader')
                            .ele('ns4:DocumentBatchId').txt('6Ms4rKRjjuMiEW40QT2uvW').up()
                            .ele('ns4:DocumentCreateDate').txt(currentTimeStamp).up()
                            const documentMetaData = documentHeader.ele('ns4:DocumentMetaData')
                                .ele('ns5:ClaimNumber').txt(alignInvoiceHeader.claim_number).up()
                                .ele('ns5:DocumentImageId').txt().up()
                                .ele('ns5:IncidentNumber').txt().up()
                                .ele('ns5:InvoiceNumber').txt(invoiceNumber).up()
                                    const recipients = documentMetaData.ele('ns5:Recipients').ele('ns5:Entry')
                                        recipients.ele('ns6:Country').txt('US').up()
                                        recipients.ele('ns6:RecipientID').txt('84f6757eac4484f6d1ff5d6812a09731').up()
                                        recipients.ele('ns6:RecipientType').txt('Agent').up()
                                    recipients.up().up()
                                .ele('ns5:RequestType').txt('AdHoc').up()
                            documentMetaData.up()
                        documentHeader.ele('ns4:DocumentType').txt('PhysicianStatement').up()
                        .up()
                        const physicianStatement = xml.ele('ns7:PhysicianStatement')
                            .ele('medicare').txt('false').up()
                            .ele('medicaid').txt('false').up()
                            .ele('champva').txt('false').up()
                            .ele('other').txt('true').up()
                            .ele('groupHealth').txt('false').up()
                            .ele('tricare').txt('false').up()
                            .ele('fecaBlklung').txt('false').up()
                            .ele('insuranceID').txt(`XXXXX-${alignInvoiceHeader.ssn?.toString().slice(-4)}` || ' ').up() // ex - xxxx 4 digits
                            .ele('patientName').txt(alignInvoiceHeader.patient_name).up()
                            if(alignInvoiceHeader.patient_date_of_birth){
                                let formattedDateOfBirth = formateTime(alignInvoiceHeader.patient_date_of_birth);
                                physicianStatement.ele('patientBirthDate').txt(formattedDateOfBirth).up()
                            } else {
                                physicianStatement.ele('patientBirthDate').txt().up()
                            }
                            physicianStatement.ele('patientM').txt(alignInvoiceHeader.patient_gender === 'M' ? 'true' : 'false').up()
                            .ele('patientF').txt(alignInvoiceHeader.patient_gender === 'F' ? 'true' : 'false').up()
                            .ele('insuredName').txt(alignInvoiceHeader.employer_name).up()
                            const patientAddress = physicianStatement.ele('patientAddress')
                                .ele('address').txt(alignInvoiceHeader.patient_address1).up()
                                .ele('city').txt(alignInvoiceHeader.patient_city).up()   
                                .ele('state').txt(alignInvoiceHeader.patient_state).up()
                                .ele('zip').txt(alignInvoiceHeader.patient_zip).up() 
                                .ele('phoneNumber').txt(alignInvoiceHeader?.patient_phone ? alignInvoiceHeader.patient_phone: '').up()
                            patientAddress.up()
                            .ele('patientRelationSelf').txt('false').up()
                            .ele('patientRelationSpouse').txt('false').up()
                            .ele('patientRelationChild').txt('false').up()
                            .ele('patientRelationOther').txt('false').up()
                            const insuredAddress = physicianStatement.ele('insuredAddress')
                                .ele('address').txt().up()
                                .ele('city').txt().up()
                                .ele('state').txt().up()
                                .ele('zip').txt().up()
                                .ele('phoneNumber').txt().up()
                            insuredAddress.up()
                            physicianStatement.ele('otherInsuredName').txt().up()
                            .ele('otherInsuredPolicyNumber').txt().up()
                            .ele('otherInsurancePlanName').txt().up()
                            .ele('patientCondEmploymentYes').txt('false').up()
                            .ele('patientCondEmploymentNo').txt('false').up()
                            .ele('patientCondAutoAccidentYes').txt('false').up()
                            .ele('patientCondAutoAccidentNo').txt('false').up()
                            .ele('patientCondOtherAccidentYes').txt('false').up()
                            .ele('patientCondOtherAccidentNo').txt('false').up()
                            .ele('patientCondPlace').txt().up()
                            .ele('otherInsuredPolicyGroup').txt(alignInvoiceHeader.claim_number).up()
                            .ele('insuredM').txt('false').up()
                            .ele('insuredF').txt('false').up()
                            .ele('otherClaimID').txt().up()
                            .ele('insurancePlanName').txt().up()
                            .ele('otherHealthPlanYes').txt('false').up()
                            .ele('otherHealthPlanNo').txt('false').up()
                            .ele('patientSignature').txt().up()
                            .ele('insuredSignature').txt().up()
                            for (let i = 0; i < totalItems; i += SERVICE_INFO_LIMIT) {
                                const hcfaFtr = physicianStatement.ele('hcfaFtr')
                                    if(alignInvoiceHeader.date_of_injury){
                                        let formattedDateOfInjury = formateTime(alignInvoiceHeader.date_of_injury);
                                        hcfaFtr.ele('dateCurrent').txt(formattedDateOfInjury).up()
                                    } else {
                                        hcfaFtr.ele('dateCurrent').txt().up()
                                    }
                                    hcfaFtr.ele('xfield17a2').txt(alignInvoiceHeader.referring_provider_state_license).up()
                                    .ele('xfield17b').txt(alignInvoiceHeader.referring_provider_npi).up()
                                    .ele('nameReferringProvider').txt(alignInvoiceHeader.referring_provider_name).up()
                                    .ele('addClaimInfo').txt().up()
                                    .ele('outsideLabYes').txt('false').up()
                                    .ele('outsideLabNo').txt('false').up()
                                    .ele('resubmissionCode').txt().up()
                                    .ele('origRefNumber').txt().up()
                                    .ele('priorAuthNumber').txt(alignInvoiceHeader.authorization_code).up()
                                    .ele('diagnosisA').txt(alignInvoiceHeader.diagnosis1).up()
                                    .ele('diagnosisB').txt(alignInvoiceHeader.diagnosis2).up()
                                    .ele('diagnosisC').txt(alignInvoiceHeader.diagnosis3).up()
                                    .ele('diagnosisD').txt(alignInvoiceHeader.diagnosis4).up()
                                    .ele('diagnosisE').txt(alignInvoiceHeader.diagnosis5).up()
                                    .ele('diagnosisF').txt(alignInvoiceHeader.diagnosis6).up()
                                    .ele('diagnosisG').txt(alignInvoiceHeader.diagnosis7).up()
                                    .ele('diagnosisH').txt(alignInvoiceHeader.diagnosis8).up()
                                    .ele('diagnosisI').txt(alignInvoiceHeader.diagnosis9).up()
                                    .ele('diagnosisJ').txt(alignInvoiceHeader.diagnosis10).up()
                                    .ele('diagnosisK').txt(alignInvoiceHeader.diagnosis11).up()
                                    .ele('diagnosisL').txt(alignInvoiceHeader.diagnosis12).up()
                                    .ele('federalTaxID').txt(alignInvoiceHeader.pay_to_fein).up()
                                    .ele('federalTaxID_SSN').txt('false').up()
                                    .ele('federalTaxID_EIN').txt('true').up()
                                    .ele('patientAcctNumber').txt(alignInvoiceHeader.invoice_number).up()
                                    .ele('acceptAssignmentYes').txt('false').up()
                                    .ele('acceptAssignmentNo').txt('false').up()
                                    .ele('totalCharge').txt(alignInvoiceHeader.invoice_total).up()
                                    .ele('signaturePhysician').txt(alignInvoiceDetail.rendering_physician_name).up()
                                    if(alignInvoiceHeader.invoice_date){
                                        let formattedSignaturePhysicianDate = formateTime(alignInvoiceHeader.invoice_date);
                                        hcfaFtr.ele('signaturePhysicianDate').txt(formattedSignaturePhysicianDate).up()
                                    } else {
                                        hcfaFtr.ele('signaturePhysicianDate').txt().up()
                                    }
                                    hcfaFtr.ele('serviceFacilityName').txt(alignInvoiceHeader.facility_name).up()
                                    .ele('serviceFacilityAddressLine').txt(alignInvoiceHeader.facility_address1).up()
                                    .ele('serviceFacilityCSZ').txt(`${alignInvoiceHeader.facility_city}, ${alignInvoiceHeader.facility_state} ${alignInvoiceHeader.facility_zip_code}`).up()
                                    .ele('billingProviderName').txt(alignInvoiceHeader.pay_to_name).up()
                                    // .ele('billingProviderAddressLine').txt(`${alignInvoiceHeader.pay_to_address1}, ${alignInvoiceHeader.pay_to_address2}`).up()
                                    .ele('billingProviderAddressLine').txt(alignInvoiceHeader.pay_to_address1 ? alignInvoiceHeader.pay_to_address2 
                                        ? `${alignInvoiceHeader.pay_to_address1}, ${alignInvoiceHeader.pay_to_address2}`
                                            : alignInvoiceHeader.pay_to_address1 : alignInvoiceHeader.pay_to_address2 || '').up()
                                    .ele('billingProviderCSZ').txt(`${alignInvoiceHeader.pay_to_city}, ${alignInvoiceHeader.pay_to_state} ${alignInvoiceHeader.pay_to_zip}`).up()
                                    .ele('physcian33pin').txt(alignInvoiceHeader.pay_to_npi).up()
                                    .ele('physiciangroup').txt(alignInvoiceDetail.rendering_physician_license_number).up()
                                    .ele('fac32a').txt(alignInvoiceHeader.facility_npi).up()
                                    .ele('fac32b').txt(alignInvoiceHeader.facility_state_licence).up()
                                    .ele('estamp').txt(currentTimeStamp).up()
                                    .ele('billingProviderPhoneNumber').txt().up();
                                    const chunk = alignInvoiceDetailResult.slice(i, i + SERVICE_INFO_LIMIT);
                                        chunk.forEach((info) => {
                                            const serviceInfo = hcfaFtr.ele('serviceInfo');
                                            if(info.start_date){
                                                const startDate = formateTime(info.start_date)
                                                serviceInfo.ele('fromDate').txt(startDate).up()
                                            } else {
                                                serviceInfo.ele('fromDate').txt().up()
                                            }
                                            if(info.end_date){
                                                const endDate = formateTime(info.end_date)
                                                serviceInfo.ele('toDate').txt(endDate).up()
                                            } else {
                                                serviceInfo.ele('toDate').txt().up()
                                            }
                                            serviceInfo.ele('placeOfService').txt(info.line_item_place_of_service).up()
                                            .ele('EMG').txt(info.line_item_emg).up()
                                            .ele('CPT_HCPCS').txt(info.hcpcs).up()
                                            .ele('modifier').txt(info.modifier).up()
                                            .ele('diagnosisPointer').txt(info.line_item_diagnosis_pointer).up()
                                            .ele('charges').txt(info.line_item_charge_amount).up()
                                            .ele('daysOrUnits').txt(info.quantity).up()
                                            .ele('familyPlan').txt(info.parent_referral_detail_id).up()
                                            .ele('idQual').txt().up()
                                            .ele('referringProviderNumber').txt(info.rendering_physician_license_number).up()
                                            .ele('referringProviderNumberNPI').txt(info.rendering_physician_npi).up()
                                            serviceInfo.up()
                                        });
                                        for (let j = chunk.length; j < SERVICE_INFO_LIMIT; j++) {
                                            const serviceInfo_1 = hcfaFtr.ele('serviceInfo')
                                                .ele('placeOfService').txt().up()
                                                .ele('EMG').txt().up()
                                                .ele('CPT_HCPCS').txt().up()
                                                .ele('modifier').txt().up()
                                                .ele('diagnosisPointer').txt().up()
                                                .ele('daysOrUnits').txt().up()
                                                .ele('familyPlan').txt().up()
                                                .ele('idQual').txt().up()
                                                .ele('referringProviderNumber').txt().up()
                                                .ele('referringProviderNumberNPI').txt().up()
                                            serviceInfo_1.up()
                                        }
                                hcfaFtr.up()
                            }
                        physicianStatement.up()
                    .up()
                .end({ prettyPrint: true });
                formattedXML = xml.end({ prettyPrint: true });
                params.Bucket = bucketName;
                params.Key = `${process.env.S3_Path}align/BITX_${alignInvoiceHeader.invoice_header_id}${timestamp}.xml`;
                params.Body = formattedXML;
                params.ContentType = 'application/xml';
                console.log('params...', params);
                break;
            case 'techhealth':
                let techQuery1 = `SELECT * FROM ${process.env.TABLE_4} WHERE invoice_header_id = ?`;
                let techQuery2 = `SELECT * FROM ${process.env.TABLE_5} WHERE invoice_header_id = ?`;
                let techQuery3 = `SELECT * FROM ${process.env.TABLE_6} WHERE invoice_header_id = ? ORDER BY CAST(line_number AS DECIMAL(10, 2)) ASC`;
                const [techInvoiceHeaderResult] = await connection.execute(techQuery1, [invoiceHeaderId]);
                const [techAttachmentResult] = await connection.execute(techQuery2, [invoiceHeaderId]);
                const [techInvoiceDetailResult] = await connection.execute(techQuery3, [invoiceHeaderId]);
                console.log('techInvoiceDetailResult',techInvoiceDetailResult);
                if (techInvoiceHeaderResult.length === 0 || techInvoiceDetailResult.length === 0) {
                    console.log('No records found');
                    return JSON.stringify({
                        status: 400,
                        message: "No records for xml generation",
                        errorType: "No records for xml generation"
                    });
                }
                const techInvoiceHeader = techInvoiceHeaderResult[0];
                console.log(techInvoiceHeader, 'techInvoiceHeader......');
                console.log(techInvoiceHeader.claim_number, 'techInvoiceHeader.claim_number......');
                const totalItems1 = Math.max(techInvoiceDetailResult.length, 1);

                // const techAttachment = techAttachmentResult[0] || '';
                const techInvoiceDetail = techInvoiceDetailResult[0];
                console.log(techInvoiceDetail, 'techInvoiceDetail.........');
                xml = create({ version: '1.0', encoding: 'UTF-8' })
                    .ele('ns7:Document', {
                        'xmlns:ns6': 'http://tmi.legacy.model.header.recipientmodel',
                        'xmlns:ns5': 'http://tmi.legacy.model.header.documentmetadatamodel',
                        'xmlns:ns7': 'http://tmi.legacy.model.documentmodel',
                        'xmlns:ns2': 'http://tmi.legacy.model.header.distributionchannelmodel',
                        'xmlns:ns4': 'http://tmi.legacy.model.header.documentheadermodel',
                        'xmlns:ns3': 'http://tmi.legacy.model.header.distributiondetailsmodel'
                    })
                        .ele('ns7:BusinessObjectType').txt('AdobeXD').up()
                        .ele('ns7:DocumentEffectiveDate').txt(currentTimeStamp).up()
                        const documentHeader1 = xml.ele('ns7:DocumentHeader')
                            .ele('ns4:DocumentBatchId').txt('6Ms4rKRjjuMiEW40QT2uvW').up()
                            .ele('ns4:DocumentCreateDate').txt(currentTimeStamp).up()
                            const documentMetaData1 = documentHeader1.ele('ns4:DocumentMetaData')
                                .ele('ns5:ClaimNumber').txt(techInvoiceHeader.claim_number).up()
                                .ele('ns5:DocumentImageId').txt('').up()
                                .ele('ns5:IncidentNumber').txt('').up()
                                .ele('ns5:InvoiceNumber').txt(invoiceNumber).up()
                                    const recipients1 = documentMetaData1.ele('ns5:Recipients').ele('ns5:Entry')
                                        .ele('ns6:Country').txt('US').up()
                                        .ele('ns6:RecipientID').txt('84f6757eac4484f6d1ff5d6812a09731').up()
                                        .ele('ns6:RecipientType').txt('Agent').up()
                                    recipients1.up().up()
                                .ele('ns5:RequestType').txt('AdHoc').up()
                            documentMetaData1.up()
                        documentHeader1.ele('ns4:DocumentType').txt('PhysicianStatement').up()
                        .up()
                        const physicianStatement1 = xml.ele('ns7:PhysicianStatement')
                            .ele('medicare').txt('false').up()
                            .ele('medicaid').txt('false').up()
                            .ele('champva').txt('false').up()
                            .ele('other').txt('true').up()
                            .ele('groupHealth').txt('false').up()
                            .ele('tricare').txt('false').up()
                            .ele('fecaBlklung').txt('false').up()
                            .ele('insuranceID').txt(`XXXXX-${techInvoiceHeader.ssn?.toString().slice(-4)}` || ' ').up()
                            .ele('patientName').txt(techInvoiceHeader.patient_name).up()
                            if(techInvoiceHeader.patient_date_of_birth){
                                let formattedDateTech = formateTime(techInvoiceHeader.patient_date_of_birth);
                                physicianStatement1.ele('patientBirthDate').txt(formattedDateTech).up()
                            } else {
                                physicianStatement1.ele('patientBirthDate').txt().up()
                            }
                            physicianStatement1.ele('patientM').txt(techInvoiceHeader.patient_gender === 'M' ? 'true' : 'false').up()
                            .ele('patientF').txt(techInvoiceHeader.patient_gender === 'F' ? 'true' : 'false').up()
                            .ele('insuredName').txt(techInvoiceHeader.employer_name).up()
                            const patientAddress1 = physicianStatement1.ele('patientAddress')
                                .ele('address').txt(techInvoiceHeader.patient_address1).up()
                     
                                .ele('city').txt(techInvoiceHeader.patient_city).up()   
                                .ele('state').txt(techInvoiceHeader.patient_state).up()
                                .ele('zip').txt(techInvoiceHeader.patient_zip).up() 
                                .ele('phoneNumber').txt(techInvoiceHeader?.patient_phone ? techInvoiceHeader.patient_phone : '')
                            patientAddress1.up()
                            physicianStatement1.ele('patientRelationSelf').txt('false').up()
                            .ele('patientRelationSpouse').txt('false').up()
                            .ele('patientRelationChild').txt('false').up()
                            .ele('patientRelationOther').txt('false').up()
                            const insuredAddress1 = physicianStatement1.ele('insuredAddress')
                        	    .ele('address').txt().up()
                        	    .ele('city').txt().up()
                        	    .ele('state').txt().up()
                        	    .ele('zip').txt().up()
                        	    .ele('phoneNumber').txt().up()
                            insuredAddress1.up()
                            physicianStatement1.ele('otherInsuredName').txt().up()
                            .ele('otherInsuredPolicyNumber').txt().up()
                            .ele('otherInsurancePlanName').txt().up()
                            .ele('patientCondEmploymentYes').txt('false').up()
                            .ele('patientCondEmploymentNo').txt('false').up()
                            .ele('patientCondAutoAccidentYes').txt('false').up()
                            .ele('patientCondAutoAccidentNo').txt('false').up()
                            .ele('patientCondOtherAccidentYes').txt('false').up()
                            .ele('patientCondOtherAccidentNo').txt('false').up()
                            .ele('patientCondPlace').txt().up()
                            .ele('otherInsuredPolicyGroup').txt(techInvoiceHeader.claim_number).up()
                            .ele('insuredM').txt('false').up()
                            .ele('insuredF').txt('false').up()
                            .ele('otherClaimID').txt().up()
                            .ele('insurancePlanName').txt().up()
                            .ele('otherHealthPlanYes').txt('false').up()
                            .ele('otherHealthPlanNo').txt('false').up()
                            .ele('patientSignature').txt().up()
                            .ele('insuredSignature').txt().up()
                            for (let i = 0; i < totalItems1; i += SERVICE_INFO_LIMIT) {
                                const hcfaFtr1 = physicianStatement1.ele('hcfaFtr')
                                    if(techInvoiceHeader.date_of_injury){
                                        let dateOfInjury = formateTime(techInvoiceHeader.date_of_injury);
                                        hcfaFtr1.ele('dateCurrent').txt(dateOfInjury).up()
                                    } else {
                                        hcfaFtr1.ele('dateCurrent').txt().up()
                                    }
                                    hcfaFtr1.ele('xfield17a2').txt(techInvoiceHeader.referring_provider_state_license).up()
                                    .ele('xfield17b').txt(techInvoiceHeader.referring_provider_npi).up()
                                    .ele('nameReferringProvider').txt(techInvoiceHeader.referring_provider_name).up()
                                    .ele('addClaimInfo').txt().up()
                                    .ele('outsideLabYes').txt('false').up()
                                    .ele('outsideLabNo').txt('false').up()
                                    .ele('resubmissionCode').txt().up()
                                    .ele('origRefNumber').txt().up()
                                    .ele('priorAuthNumber').txt(techInvoiceHeader.authorization_code).up()
                                    .ele('diagnosisA').txt(techInvoiceHeader.diagnosis1).up()
                                    .ele('diagnosisB').txt(techInvoiceHeader.diagnosis2).up()
                                    .ele('diagnosisC').txt(techInvoiceHeader.diagnosis3).up()
                                    .ele('diagnosisD').txt(techInvoiceHeader.diagnosis4).up()
                                    .ele('diagnosisE').txt(techInvoiceHeader.diagnosis5).up()
                                    .ele('diagnosisF').txt(techInvoiceHeader.diagnosis6).up()
                                    .ele('diagnosisG').txt(techInvoiceHeader.diagnosis7).up()
                                    .ele('diagnosisH').txt(techInvoiceHeader.diagnosis8).up()
                                    .ele('diagnosisI').txt(techInvoiceHeader.diagnosis9).up()
                                    .ele('diagnosisJ').txt(techInvoiceHeader.diagnosis10).up()
                                    .ele('diagnosisK').txt(techInvoiceHeader.diagnosis11).up()
                                    .ele('diagnosisL').txt(techInvoiceHeader.diagnosis12).up()
                                    .ele('federalTaxID').txt(techInvoiceHeader.pay_to_fein).up()
                                    .ele('federalTaxID_SSN').txt('false').up()
                                    .ele('federalTaxID_EIN').txt('true').up()
                                    .ele('patientAcctNumber').txt(techInvoiceHeader.invoice_number).up()
                                    .ele('acceptAssignmentYes').txt('false').up()
                                    .ele('acceptAssignmentNo').txt('false').up()
                                    .ele('totalCharge').txt(techInvoiceHeader.invoice_total).up()
                                    .ele('signaturePhysician').txt(techInvoiceDetail.rendering_physician_name).up()
                                    if(techInvoiceHeader.invoice_date){
                                        let formattedSignaturePhysicianDate = formateTime(techInvoiceHeader.invoice_date);
                                        hcfaFtr1.ele('signaturePhysicianDate').txt(formattedSignaturePhysicianDate).up()
                                    } else {
                                        hcfaFtr1.ele('signaturePhysicianDate').txt().up()
                                    }
                                    hcfaFtr1.ele('serviceFacilityName').txt(techInvoiceHeader.facility_name).up()
                                    .ele('serviceFacilityAddressLine').txt(techInvoiceHeader.facility_address1).up()
                                    .ele('serviceFacilityCSZ').txt(`${techInvoiceHeader.facility_city}, ${techInvoiceHeader.facility_state} ${techInvoiceHeader.facility_zip_code}`).up()
                                    .ele('billingProviderName').txt(techInvoiceHeader.pay_to_name).up()
                                    // .ele('billingProviderAddressLine').txt(`${techInvoiceHeader.pay_to_address1}, ${techInvoiceHeader.pay_to_address2}`).up()
                                    .ele('billingProviderAddressLine').txt(techInvoiceHeader.pay_to_address1 ? techInvoiceHeader.pay_to_address2 
                                        ? `${techInvoiceHeader.pay_to_address1}, ${techInvoiceHeader.pay_to_address2}`
                                            : techInvoiceHeader.pay_to_address1
                                        : techInvoiceHeader.pay_to_address2 || '').up()
                                    .ele('billingProviderCSZ').txt(`${techInvoiceHeader.pay_to_city}, ${techInvoiceHeader.pay_to_state} ${techInvoiceHeader.pay_to_zip}`).up()
                                    .ele('physcian33pin').txt(techInvoiceHeader.pay_to_npi).up()                                
                                    .ele('physiciangroup').txt(techInvoiceDetail.rendering_physician_license_number).up()
                                    .ele('fac32a').txt(techInvoiceHeader.facility_npi).up()
                                    .ele('fac32b').txt(techInvoiceHeader.facility_state_licence).up()
                                    .ele('estamp').txt(currentTimeStamp).up()
                                    .ele('billingProviderPhoneNumber').txt().up();
                                    const chunk = techInvoiceDetailResult.slice(i, i + SERVICE_INFO_LIMIT);
                                    chunk.forEach((info) => {
                                        const serviceInfo1 = hcfaFtr1.ele('serviceInfo')
                                            if(info.start_date){
                                                const startDate = formateTime(info.start_date)
                                                serviceInfo1.ele('fromDate').txt(startDate).up()
                                            } else {
                                                serviceInfo1.ele('fromDate').txt().up()
                                            }
                                            if(info.end_date){
                                                const endDate = formateTime(info.end_date)
                                                serviceInfo1.ele('toDate').txt(endDate).up()
                                            } else {
                                                serviceInfo1.ele('toDate').txt().up()
                                            }
                                            serviceInfo1.ele('placeOfService').txt(info.line_item_place_of_service).up()
                                            .ele('EMG').txt(info.line_item_emg).up()
                                            .ele('CPT_HCPCS').txt(info.hcpcs).up()
                                            .ele('modifier').txt(info.modifier).up()
                                            .ele('diagnosisPointer').txt(info.line_item_diagnosis_pointer).up()
                                            .ele('charges').txt(info.line_item_charge_amount).up()
                                            .ele('daysOrUnits').txt(info.quantity).up()
                                            .ele('familyPlan').txt(info.parent_referral_detail_id).up()
                                            .ele('idQual').txt().up()
                                            .ele('referringProviderNumber').txt(info.rendering_physician_license_number).up()
                                            .ele('referringProviderNumberNPI').txt(info.rendering_physician_npi).up()
                                        serviceInfo1.up()
                                        });
                                        for (let j = chunk.length; j < SERVICE_INFO_LIMIT; j++) {
                                            const serviceInfo2 = hcfaFtr1.ele('serviceInfo')
                                                    .ele('placeOfService').txt().up()
                                                    .ele('EMG').txt().up()
                                                    .ele('CPT_HCPCS').txt().up()
                                                    .ele('modifier').txt().up()
                                                    .ele('diagnosisPointer').txt().up()
                                                    .ele('daysOrUnits').txt().up()
                                                    .ele('familyPlan').txt().up()
                                                    .ele('idQual').txt().up()
                                                    .ele('referringProviderNumber').txt().up()
                                                    .ele('referringProviderNumberNPI').txt().up()
                                            serviceInfo2.up()
                                        }   
                                hcfaFtr1.up()
                            }
                        physicianStatement1.up()
                    .up()
                .end({ prettyPrint: true });
                formattedXML = xml.end({ prettyPrint: true });
                params.Bucket = bucketName;
                params.Key = `${process.env.S3_Path}techhealth/BITX_${techInvoiceHeader.invoice_header_id}${timestamp}.xml`;
                params.Body = formattedXML;
                params.ContentType = 'application/xml';
                console.log('params...', params);
                break;
            case 'optum':
                let optumQuery = `SELECT * FROM ${process.env.TABLE_7} WHERE invoice_header_id = ?`;
                const [optumResult] = await connection.execute(optumQuery, [invoiceHeaderId]);
                if (optumResult.length === 0) {
                    console.log("No records found");
                    return JSON.stringify({
                        status: 400,
                        message: "No records for xml generation",
                        errorType: "No records for xml generation"
                    });
                }
                const optumData = optumResult[0];
                console.log('optumData...', optumData);
                let optumClaimDataQuery = `SELECT * FROM ${process.env.TABLE_8} WHERE trading_partner = 'optum' and  txm_claim_number= ?;`;
                const [optumClaimDataResult] = await connection.execute(optumClaimDataQuery, [optumData.claim_number]);
                console.log('optumClaimDataResult.....',optumClaimDataResult[0]);

                let pharmacyLineQuery = `SELECT * FROM ${process.env.DB_2}.${process.env.TABLE_9} WHERE bill_id = ?  ORDER BY CAST(line_number AS DECIMAL(10, 2)) ASC`;
                const [pharmacyLineResult] = await connection.execute(pharmacyLineQuery, [billId]);
                console.log('pharmacyLineResult.....',pharmacyLineResult);

                const claimData = optumClaimDataResult[0];
                let dateOptum = optumData.employee_dob;
                let date1 = optumData.date_of_injury;
                let date2 = optumData.date_written;
                let date3 = optumData.date_filled;
                console.log('dateOptum...', dateOptum);
                let formattedDateOptum = formateTime(dateOptum)//dateOptum.toISOString().slice(0, 29);
                let formattedDate1 = formateTime(date1)//date1.toISOString().slice(0, 29);
                let formattedDate2 = formateTime(date2)//date2.toISOString().slice(0, 29);
                let date_filled = formateTime(date3)//date3.toISOString().slice(0, 29);
                console.log('formattedDate1...', formattedDate1);
                console.log('formattedDate2...', formattedDate2);
                xml = create({ version: '1.0', encoding: 'UTF-8' })
                    .ele('ns7:Document', {
                        'xmlns:ns6': 'http://tmi.legacy.model.header.recipientmodel',
                        'xmlns:ns5': 'http://tmi.legacy.model.header.documentmetadatamodel',
                        'xmlns:ns7': 'http://tmi.legacy.model.documentmodel',
                        'xmlns:ns2': 'http://tmi.legacy.model.header.distributionchannelmodel',
                        'xmlns:ns4': 'http://tmi.legacy.model.header.documentheadermodel',
                        'xmlns:ns3': 'http://tmi.legacy.model.header.distributiondetailsmodel'
                    })
                    .ele('ns7:BusinessObjectType').txt('AdobeXD').up()
                    .ele('ns7:DocumentEffectiveDate').txt(currentTimeStamp).up()
                    .ele('ns7:DocumentHeader')
                    .ele('ns4:DocumentBatchId').txt('4tiIwuhqPLVp5gSOe5NhVU').up()
                    .ele('ns4:DocumentCreateDate').txt(currentTimeStamp).up()
                    .ele('ns4:DocumentMetaData')
                    .ele('ns5:ClaimNumber').txt(optumData.claim_number).up()
                    .ele('ns5:DocumentImageId').txt('').up()
                    .ele('ns5:InvoiceNumber').txt(invoiceNumber).up()
                    .ele('ns5:Recipients')
                    .ele('ns5:Entry')
                    .ele('ns6:Country').txt('US').up()
                    .ele('ns6:RecipientID').txt('fbc0821bc9342d7e0bf261136c335477').up()
                    .ele('ns6:RecipientType').txt('Agent').up()
                    .up()
                    .up()
                    .ele('ns5:RequestType').txt('AdHoc').up()
                    .up()
                    .ele('ns4:DocumentType').txt('PharmacyStatement').up()
                    .up()
                    .ele('ns7:PharmacyStatement')
                    .ele('FormSectionOne')
                    .ele('pharmacyAddress')
                    .ele('addressTo').txt(optumData.pharmacy_name).up()
                    .ele('addressOne').txt(`${optumData.pharmacy_address1? optumData.pharmacy_address1 : ''} ${optumData.pharmacy_address2 ? `, ${optumData.pharmacy_address2}` : ''}`).up()
                    .ele('city').txt(optumData.pharmacy_city).up()
                    .ele('state').txt(optumData.pharmacy_state).up()
                    .ele('zip').txt(optumData.pharmacy_zip ? formatZipCode(optumData.pharmacy_zip): '').up()
                    .ele('phoneNumber').txt(optumData.pharmacy_phone_number ? `${optumData.pharmacy_phone_number?.toString().slice(0, 3)}-${optumData.pharmacy_phone_number?.toString().slice(3, 6)}-${optumData.pharmacy_phone_number?.toString().slice(6)}` : '').up()
                    .up()
                    .ele('dateOfBilling').txt(formattedDate2).up()
                    .ele('pharmacyNpiNumber').txt(optumData.dispensing_pharmacy_npi).up()
                    .ele('pharmacyNcpdp').txt(optumData.nabp).up()
                    .ele('remitPaymentAddress')
                    .ele('addressTo').txt(optumData.sn_name ==='TMESYS' ? 'TMESYS LLC' : optumData.sn_name).up()
                    .ele('addressOne').txt(optumData.sn_address1).up()
                    .ele('city').txt(optumData.sn_city).up()
                    .ele('state').txt(optumData.sn_state).up()
                    .ele('zip').txt(optumData.sn_zip_code ? formatZipCode(optumData.sn_zip_code): '').up()
                    .up()
                    .ele('invoiceNumber').txt(invoiceNumber).up()
                    .ele('payeeFein').txt(optumData.irs_number).up()
                    .ele('carrierAddress')
                    .ele('addressTo').txt('Texas Mutual Insurance Company').up()
                    .ele('addressOne').txt('P.O. Box 12029').up()
                    .ele('city').txt('Austin').up()
                    .ele('state').txt('TX').up()
                    .ele('zip').txt('78711-2029').up()
                    .up()
                    .ele('employerAddress')
                    .ele('addressTo').txt(claimData.subscriber_name).up()
                    .ele('addressOne').txt(claimData.subscriber_address_line_1).up()
                    .ele('city').txt(claimData.subscriber_city).up()
                    .ele('state').txt(claimData.subscriber_state).up()
                    .ele('zip').txt(claimData.subscriber_zip_code ? formatZipCode(claimData.subscriber_zip_code) : '').up()
                    // .ele('phoneNumber').txt(claimData.subscriber_telephone_number).up()   //.txt(`${optumData.employer_phone_number?.toString().slice(0, 3)}-${optumData.employer_phone_number?.toString().slice(3, 6)}-${optumData.employer_phone_number?.toString().slice(6)} || ''`)
                    .ele('phoneNumber').txt(claimData.subscriber_telephone_number ? `${claimData.subscriber_telephone_number?.toString().slice(0, 3)}-${claimData.subscriber_telephone_number?.toString().slice(3, 6)}-${claimData.subscriber_telephone_number?.toString().slice(6)}` : '').up()   
                    .up()
                    .ele('employeeAddress')
                    .ele('addressTo').txt(`${optumData.member_last_name ? optumData.member_last_name : ''}, ${optumData.member_first_name? optumData.member_first_name : ''}`).up()
                    .ele('addressOne').txt(claimData.patient_address_line_1).up()
                    .ele('city').txt(claimData.patient_city).up()
                    .ele('state').txt(claimData.patient_state_code).up()
                    .ele('zip').txt(claimData.patient_zip_code ? formatZipCode(claimData.patient_zip_code) : '').up()
                    .ele('phoneNumber').txt(claimData.patient_phone_number ? `${claimData.patient_phone_number?.toString().slice(0, 3)}-${claimData.patient_phone_number?.toString().slice(3, 6)}-${claimData.patient_phone_number?.toString().slice(6)}` : '').up()
                    .up()
                    .ele('prescribingAddress')
                    .ele('addressTo').txt(`${optumData.physician_last_name ? optumData.physician_last_name : ''}, ${optumData.physician_first_name ? optumData.physician_first_name : ''} ${optumData.physician_middle_init ? optumData.physician_middle_init : ''}`).up()
                    .ele('addressOne').txt(optumData.physician_address).up()
                    .ele('city').txt(optumData.physician_city).up()
                    .ele('state').txt(optumData.physician_state).up()
                    .ele('zip').txt(optumData.physician_zip_code ? formatZipCode(optumData.physician_zip_code) : '').up()
                    .ele('phoneNumber').up()
                    .up()
                    .ele('injuredEmployeeId').txt(`XXX-XX-${optumData.ssn?.toString().slice(-4)}` || '').up()
                    .ele('jurisdiction').txt('TX').up()
                    .ele('ssnFlag').txt('X').up()
                    .ele('dlFlag').up()
                    .ele('greenCardFlag').up()
                    .ele('passportFlag').up()
                    .ele('doctorNpiNumber').txt(optumData.prescriber_npi).up()
                    .ele('deaNumber').txt(optumData.dea).up()
                    .ele('dateOfInjury').txt(formattedDate1).up()
                    .ele('dateOfBirth').txt(formattedDateOptum).up()
                    .ele('serialNumber').up()
                    .ele('carrierClaimNumber').txt(optumData.claim_number).up()
                    .up()
                    pharmacyLineResult.forEach((item) => {
                        const formSectionTwo = xml.ele('FormSectionTwo');
                        formSectionTwo.ele('dispensedGenericFlag').txt(item.generic_dispensed === 'Y' ? 'X' : '').up()
                        .ele('dispensedNameBrandFlag').txt((item.generic_dispensed === 'O' || item.generic_dispensed === 'M' || item.generic_dispensed === 'N') ? 'X' : '').up()
                        if(item.generic_dispensed === 'O' || item.generic_dispensed === 'M' || item.generic_dispensed === 'N'){
                            if(optumData.generic_available_ndc > ''){
                                formSectionTwo.ele('availableGenericFlagYes').txt('X').up()
                                .ele('availableGenericFlagNo').txt().up()
                            } else {
                                formSectionTwo.ele('availableGenericFlagYes').txt().up()
                                .ele('availableGenericFlagNo').txt('X').up()
                            }
                        } else {
                            formSectionTwo.ele('availableGenericFlagYes').txt().up()
                            .ele('availableGenericFlagNo').txt().up()
                        }
                        formSectionTwo.ele('dispensedAsWrittenCode').txt(item. dispensed_as_written_code).up()
                        .ele('dateFilled').txt(formateTime(item.prescription_date)).up()
                        
                        if(item.generic_dispensed === 'Y' ){
                            formSectionTwo.ele('genericNdc').txt(item. ndc_code).up()
                        } else {
                            formSectionTwo.ele('genericNdc').txt().up()
                        }
                        if(item.generic_dispensed === 'O' || item.generic_dispensed === 'M' || item.generic_dispensed === 'N'){
                            formSectionTwo.ele('nameBrandNdc').txt(item. ndc_code).up()
                        } else {
                            formSectionTwo.ele('nameBrandNdc').txt().up()
                        }
                        formSectionTwo.ele('quantity').txt(item.quantity).up()
                        .ele('daysSupply').txt(item.days_supply).up()
                        .ele('refillsRemaining').txt(item.fill_count).up()
                        .ele('paidByEmployee').txt(item.recommended_amount).up()
                        .ele('drugNameAndStrength').txt(item.drug_name).up()
                        .ele('rxNumber').txt(item. prescription_number).up()
                        .ele('amountBilled').txt(item.charged_amount).up()
                        formSectionTwo.up()
                    });
                    // .up()
                    
                formattedXML = xml.end({ prettyPrint: true });;
                params.Bucket = bucketName;
                params.Key = `${process.env.S3_Path}optum/BITX_${optumData.invoice_header_id}${timestamp}.xml`;
                params.Body = formattedXML;
                params.ContentType = 'application/xml';
                console.log('params...', params);
                break;
            default:
                throw new Error('Unsupported trading partner');
        }
        // console.log('xml...', xml.end({prettyPrint: true}));
        console.log('xml', formattedXML);
        // let response = null;
        try {
            await s3.upload(params).promise();
            console.log("Successfully uploaded XML to S3");
            // await connection.query(`UPDATE ${process.env.DB_2}.billing_documents SET bill_xml_document_path = 's3://${bucketName}/${params.Key}' WHERE bill_id = ${billId}`);
            const now = new Date();
            let createdOn = now.toISOString().slice(0, 19).replace('T', ' ');
            let updatedOn = createdOn;
            let s3Filepath = `s3://${bucketName}/${params.Key}`;
            await connection.query(`INSERT INTO ${process.env.DB_2}.billing_documents (bill_id, file_type, bill_xml_document_path, create_user, create_timestamp, update_timestamp) VALUES (?, ?, ?, ?, ?, ?)`, [billId, 'XML', s3Filepath, 'LAMBDA', createdOn, updatedOn]);

        } catch (error) {
            console.log("Error occurred while uploading XML to S3", error);
            // throw new Error("Error occurred while uploading XML to S3", error);
            return JSON.stringify({
                status: error.statusCode,
                message: error.code,
                errorType: "Internal server error"
            });
        }
        return JSON.stringify({
            status: 200,
            body: { message: "Successfully created xml" },
        });
    } catch (err) {
        console.log('error message last catch', err);
        // throw new Error("Error message", err);
        return JSON.stringify({
            status: err.statusCode,
            message: err.code,
            errorType: "Internal server error"
        });
    }
};

const formateTime = (value) => {
    const date = new Date(value);
    const offset = -date.getTimezoneOffset();
    const sign = offset >= 0 ? "+" : "-";
    const hours = String(Math.abs(offset / 60)).padStart(2, "0");
    const minutes = String(Math.abs(offset % 60)).padStart(2, "0");
  
    return date.toISOString().slice(0, -1) + sign + hours + ":" + minutes;
  }

const formatZipCode = (input) => {
    const strInput = String(input);
    if (strInput.includes('-') || strInput.length <= 5) {
      return strInput;
    }
    return strInput.slice(0, 5) + "-" + strInput.slice(5);
  }