/* Evaluation_List_Cancelled SQL query */
WITH CFNC_IVTG_W AS (
    SELECT /*+ INDEX(A IX_CFNC_IVTG_M_01) INDEX(B PK_CFNC_LOAN_M) INDEX(G PK_CFNC_BGDS_I) INDEX(F PK_CFNC_IVPG_L) */
           A.MBCM_NO                  /* 회원사번호 */ 
         , A.LOAN_NO                  /* 대출번호 */
         , A.IVTG_DMAN_DT             /* 심사요청일자 */
         , A.IVTG_PRGS_STCD           /* 심사진행상태코드 */
         , A.LAST_IVTG_PRGS_SEQ       /* 최종심사진행순번 */
         , A.XCPT_APRV_DMAN_DT        /* 예외승인요청일자 */
         , A.XCPT_APRV_DMAN_YN        /* 예외승인요청여부 */
         , A.IVTG_BRCD                /* 심사지점코드 */
         , A.IVER_USER_NO             /* 심사자사용자번호 */
         , B.CSTNO                    /* 고객번호 */
         , B.IDVD_CRPT_DVCD           /* 개인법인구분코드 */
         , CASE WHEN B.LOAN_PRGS_STEP_CD = '20' AND A.IVTG_PRGS_STCD = '99' THEN '21'
                WHEN B.LOAN_PRGS_STEP_CD = '20' AND A.IVTG_PRGS_STCD = '97' THEN '22'
                ELSE B.LOAN_PRGS_STEP_CD
           END AS LOAN_PRGS_STEP_CD   /* 대출진행단계코드 */
         , B.SALE_GDS_CD              /* 매출상품코드 */
         , G.LOAN_GDS_LCCD            /* 대출상품대분류코드 */
         , G.LOAN_GDS_MCCD            /* 대출상품중분류코드 */
         , G.LOAN_GDS_SCCD            /* 대출상품소분류코드 */
         , B.AGRE_CD                  /* 약정코드 */
         , B.LNAM                     /* 대출금액 */
         , B.LOAN_TOT_FEE_AMT         /* 대출총수수료금액 */
         , B.LOAN_BRCD                /* 대출지점코드 */
         , B.LOAN_DT                  /* 대출일자 */
         , B.FRST_REG_DT              /* 최초등록일자 */
         , B.FRST_REG_TIME            /* 최초등록시각 */
         , B.FRST_REG_USER_NO         /* 최초등록사용자번호 */
         , B.BFHD_ACTC_APPC_NO        /* 사전접수신청번호 */
         , B.OBD_CHNL_DVCD            /* 온보딩채널구분코드 */
         , B.FRM_DT                   /* 확정일자 */
         , B.LNAL_ACTC_DT             /* 대출신청접수일자 */
         , F.IVTG_FIN_DT              /* 심사완료일자 */
         , F.INS_FNNC_IVTG_RSLT_CD    /* 할부금융심사결과코드 */
         , DECODE(F.INS_FNNC_IVTG_RSLT_CD, '04', F.IVTG_CANC_RSCD, NULL)  AS IVTG_DENL_RSCD   /* 심사거절사유코드 */
         , DECODE(F.INS_FNNC_IVTG_RSLT_CD, '05', F.IVTG_CANC_RSCD, NULL)  AS IVTG_RTRC_RSCD   /* 심사반려사유코드 */
         , DECODE(F.INS_FNNC_IVTG_RSLT_CD, '06', F.IVTG_CANC_RSCD, NULL)  AS IVTG_CANC_RSCD   /* 심사취소사유코드 */
         , DECODE(F.INS_FNNC_IVTG_RSLT_CD, '07', F.IVTG_CANC_RSCD, NULL)  AS IVTG_WAIT_RSCD   /* 심사대기사유코드 */
         , G.FSTK_APRV_YN             /* FastTrack승인여부 */
      FROM CFNC_IVTG_M A   /* FNC_심사기본 */
      JOIN CFNC_LOAN_M B   /* FNC_대출기본 */
        ON A.MBCM_NO = B.MBCM_NO
       AND A.LOAN_NO = B.LOAN_NO
      JOIN CFNC_BGDS_I G   /* FNC_매출상품정보 */
        ON B.AGRE_CD = G.AGRE_CD
       AND B.LNAL_ACTC_DT BETWEEN G.GDS_APLY_STDT AND G.GDS_APLY_ENDT
      LEFT OUTER JOIN CFNC_IVPG_L F   /* FNC_심사진행내역 */
        ON A.MBCM_NO = F.MBCM_NO
       AND A.LOAN_NO = F.LOAN_NO
       AND A.LAST_IVTG_PRGS_SEQ = F.IVTG_PRGS_SEQ
     WHERE 1=1
       AND A.MBCM_NO = '855'
       AND A.IVTG_PRGS_STCD ='97'
       AND A.IVTG_DMAN_DT   BETWEEN TO_CHAR(TRUNC(SYSDATE) , 'YYYYMMDD')  AND TO_CHAR(TRUNC(SYSDATE), 'YYYYMMDD') 
       AND B.SALE_GDS_CD       <> 'A701'
       AND B.CANC_YN           <> 'Y'
       AND B.DEL_YN            <> 'Y'
       AND B.LOAN_PRGS_STEP_CD IN ('20', '29', '30', '40', '41')  /* 20:심사,29:대출확정,30:대금지급,40:완납,41:감면완납 */
     ORDER BY CASE WHEN B.OBD_CHNL_DVCD  IN ('02','03') AND F.INS_FNNC_IVTG_RSLT_CD = '05' THEN TO_NUMBER( '1'    || A.IVTG_DMAN_DT || A.FRST_REG_TIME )
                   WHEN A.IVTG_PRGS_STCD IN ('00','97','99')                               THEN TO_NUMBER( '1001' || A.IVTG_DMAN_DT || A.FRST_REG_TIME )
                   WHEN G.FSTK_APRV_YN = 'Y'                                               
                   THEN TO_NUMBER(A.IVTG_PRGS_STCD || DECODE(F.INS_FNNC_IVTG_RSLT_CD, ' ', '1', '3') || A.IVTG_DMAN_DT || A.FRST_REG_TIME )
                   ELSE TO_NUMBER(A.IVTG_PRGS_STCD || DECODE(F.INS_FNNC_IVTG_RSLT_CD, ' ', '2', '4') || A.IVTG_DMAN_DT || A.FRST_REG_TIME ) 
               END
)
SELECT /*+ INDEX(H PK_CCMM_USER_M) INDEX(C PK_CCMM_CODE_D) */
       T.IVER_USER_NO||DECODE(T.IVER_USER_NO,' ','', ' : ')|| H.USER_NM AS Evaluator
     , T.OBD_CHNL_DVCD  AS "On-boarding Channel"
     , T.SALE_GDS_CD         AS "Product Code"
     , T.IVTG_DMAN_DT      AS "Requested date"
     , N.FRST_REG_TIME      AS "Request time"
     , T.CSTNO                       AS "Customer No"
     , K.CUST_NM                  AS "Customer Name"
     , R.RNNO                         AS "Customer ID No."
     , T.LOAN_NO                  AS "Loan No." 
     , T.IVTG_PRGS_STCD    AS "Evaluating progress status"
     , T.AGRE_CD||' : '||C.STND_CD_VAL_DESC   AS "Project Code"
     , T.LNAM                         AS "Loan Amt."
     , T.BFHD_ACTC_APPC_NO AS "Pre-Application No"
     , J.CMCP_IVTG_RSLT_CD    AS "CSS evaluation result"	
     , T.INS_FNNC_IVTG_RSLT_CD AS "Final Result"
     , T.IVTG_RTRC_RSCD    AS "Return reason"
     , T.IVTG_CANC_RSCD   AS "Cancellation Reason"			
     , T.IVTG_DENL_RSCD    AS "Decline reason"                          
     , T.IVTG_WAIT_RSCD    AS "Pending Reason"
     , T.IVTG_FIN_DT             AS "Evaluation Date"
     , T.FRM_DT                      AS "Confirmation date"
  FROM CFNC_IVTG_W T
  LEFT OUTER JOIN CCMM_CODE_D C   /* CMM_표준코드상세 - 프로젝트코드 */
    ON T.AGRE_CD = C.STND_CD_VAL
   AND C.STND_CD_NM = 'AGRE_CD'
  LEFT OUTER JOIN CCMM_USER_M H   /* CMM_사용자기본 */
    ON T.MBCM_NO = H.MBCM_NO
   AND T.IVER_USER_NO = H.USER_NO
  LEFT OUTER JOIN CFNC_CMIV_L J   /* 전산심사내역 */
    ON T.MBCM_NO = J.MBCM_NO
   AND T.LOAN_NO = J.CMCP_IVTG_TRGT_NO 
  LEFT OUTER JOIN CCST_CSNM_L K   /* 고객명내역 */
    ON K.MBCM_NO = T.MBCM_NO
   AND K.CSTNO = T.CSTNO
   AND K.CUST_NM_DVCD = '1' 
  LEFT OUTER JOIN CFNC_IVPG_L N 
    ON N.MBCM_NO = T.MBCM_NO
   AND N.LOAN_NO = T.LOAN_NO
   AND N.IVTG_PRGS_SEQ = (SELECT MAX(SEQ) 
                            FROM (SELECT DECODE((LAG(NN.IVTG_PRGS_STCD) OVER (ORDER BY NN.IVTG_PRGS_SEQ)), '61', NN.IVTG_PRGS_SEQ, 1) AS SEQ 
                                    FROM CFNC_IVPG_L NN 
                                   WHERE NN.MBCM_NO = T.MBCM_NO 
                                     AND NN.LOAN_NO = T.LOAN_NO))
  JOIN CCST_RNNO_M R   /* CST_고객실명기본 */
    ON T.MBCM_NO = R.MBCM_NO
   AND T.CSTNO = R.CSTNO