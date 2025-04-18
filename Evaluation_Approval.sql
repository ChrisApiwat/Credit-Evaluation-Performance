/* Evaluation_Approval SQL query */
WITH CFNC_IVAP_W AS (
    SELECT /*+ INDEX (E IX_CFNC_IVAP_L_01) INDEX (A PK_CFNC_LOAN_M) */
           A.MBCM_NO                        /* 회원사번호 */
         , A.LOAN_NO                        /* 대출번호 */
         , A.CSTNO                          /* 고객번호 */
         , A.PYT_DMAN_NO                    /* 지불요청번호 */
         , A.BFHD_ACTC_APPC_NO              /* 사전접수신청번호 */
         , A.IDVD_CRPT_DVCD                 /* 개인법인구분코드 */
         , A.CSGR_DVCD                      /* 고객그룹구분코드 */
         , A.LNAL_ACTC_DT                   /* 대출신청접수일자 */
         , A.LNAL_ACTC_SEQ                  /* 대출신청접수순번 */
         , A.INS_LEAS_DVCD                  /* 할부리스구분코드 */
         , A.SALE_GDS_CD                    /* 매출상품코드 */
         , A.CPST_DVCD                      /* 제휴점구분코드 */
         , A.AGRE_CD                        /* 약정코드 */
         , A.PROMT_CD                       /* 프로모션코드 */
         , A.MBL_PROMT_CD                   /* 모바일프로모션코드 */
         , A.CPST_NO                         /* 제휴점번호 */
         , A.LOAN_TRM_MCNT                   /* 대출기간개월수 */
         , A.DFRM_TRM_MCNT                   /* 거치기간개월수 */
         , A.EFEC_IRATE                      /* 효과이자율 */
         , A.FLAT_IRATE                      /* FLAT이자율 */
         , A.LNAM                            /* 대출금액 */
         , A.TINT_AMT                        /* 총이자금액 */
         , A.MM_PYMN_AMT                     /* 월불입금액 */
         , A.RPRS_REPAY_MTCD                 /* 대표상환방법코드 */
         , A.REPAY_TYCD                      /* 상환유형코드 */
         , A.LOAN_BRCD                       /* 대출지점코드 */
         , A.LOAN_DT                         /* 대출일자 */
         , A.EXPR_DT                         /* 만기일자 */
         , A.FRM_DT                          /* 확정일자 */
         , A.SETL_DD                         /* 결제일 */
         , A.FRST_SETL_DT                    /* 최초결제일자 */
         , A.LSTL_DT                         /* 최종결제일자 */
         , A.LAST_DMND_NTH                   /* 최종청구회차 */
         , A.LSRC_DT                         /* 최종수납일자 */
         , A.LOAN_TOT_FEE_AMT                /* 대출총수수료금액 */
         , A.PPAY_AMT                        /* 선납금액 */
         , A.PRCV_RATE                       /* 선수율 */
         , A.SSPN_AMT                        /* 유예금액 */
         , A.SSPN_RATE                       /* 유예율 */
         , A.POCP_INS_AMT                    /* 선취할부금액 */
         , A.LAST_MNRC_AMT                   /* 최종입금금액 */
         , A.LOAN_BAL                        /* 대출잔액 */
         , A.PRCV_BAL                        /* 선수잔액 */
         , A.FLPY_YN                         /* 완납여부 */
         , A.FLPY_DT                         /* 완납일자 */
         , A.DTST_ACCP_MTCD                  /* 명세서수령방법코드 */
         , A.DTST_ADDR                       /* 명세서주소 */
         , CASE WHEN A.LOAN_PRGS_STEP_CD = '20' AND C.IVTG_PRGS_STCD = '99' THEN '21'
                WHEN A.LOAN_PRGS_STEP_CD = '20' AND C.IVTG_PRGS_STCD = '97' THEN '22'
                ELSE A.LOAN_PRGS_STEP_CD
            END AS LOAN_PRGS_STEP_CD /* 대출진행단계코드 */
         , A.CANC_YN                         /* 취소여부 */
         , A.CANC_DT                         /* 취소일자 */
         , A.LOAN_CANC_MEMO_CTNT             /* 대출취소메모내용 */
         , A.DEL_YN                          /* 삭제여부 */
         , A.LOAN_PRPS_RSCD                  /* 대출목적사유코드 */
         , C.IVTG_DMAN_DT                    /* 심사요청일자 */
         , C.IVTG_PRGS_STCD                  /* 심사진행상태코드 */
         , C.LAST_IVTG_PRGS_SEQ              /* 최종심사진행순번 */
         , C.XCPT_APRV_DMAN_DT               /* 예외승인요청일자 */
         , C.XCPT_APRV_DMAN_YN               /* 예외승인요청여부 */
         , C.IVTG_JBTY_CD                    /* 심사직군코드 */
         , C.IVTG_PSTN_CD                    /* 심사직위코드 */
         , C.DBTR_IDNO                       /* 채무자식별번호 */
         , C.IVTG_BRCD                       /* 심사지점코드 */
         , C.IVER_USER_NO                    /* 심사자사용자번호 */
         , C.BROF_OPN_CTNT                   /* 지점의견내용 */
         , D.IVTG_PRGS_SEQ                   /* 심사진행순번 */
         , D.IVTG_DMAN_CHGR_USER_NO          /* 심사요청담당자사용자번호 */
         , D.IVER_AOCT_DT                    /* 심사자할당일자 */
         , D.IVTG_STEP_CD                    /* 심사단계코드 */
         , D.IVTG_FIN_DT                     /* 심사완료일자 */
         , D.LAST_APRVL_STEP_CD              /* 최종결재단계코드 */
         , D.INS_FNNC_IVTG_RSLT_CD           /* 할부금융심사결과코드 */
         , D.IVTG_LVOT_RSCD                  /* 심사탈락사유코드 */
         , E.IVTG_APRVL_SEQ                  /* 심사결재순번 */
         , E.APRVL_DMAN_DT                   /* 결재요청일자 */
         , E.APRVL_STEP_CD                   /* 결재단계코드 */
         , E.APRVL_STCD                      /* 결재상태코드 */
         , E.APRVL_KNCD                      /* 결재종류코드 */
         , E.INS_FNNC_APRVL_FIN_DT        /* 할부금융결재완료일자 */
         , E.APOF_USER_NO                    /* 결재자사용자번호 */
         , E.IVTG_APRVL_RSLT_CD              /* 심사결재결과코드 */
         , E.APRVL_OPN_CTNT                  /* 결재의견내용 */
         , J.FSTK_APRV_YN                    /* FastTrack승인여부 */
         , J.LOAN_GDS_LCCD                   /* 대출상품대분류코드 */
         , J.LOAN_GDS_MCCD                   /* 대출상품중분류코드 */
         , J.LOAN_GDS_SCCD                   /* 대출상품소분류코드 */
         , CASE WHEN C.IVTG_PRGS_STCD IN ('00:Confirmed', '97', '99:Rejected') THEN 1001
                WHEN J.FSTK_APRV_YN = 'Y'                   THEN TO_NUMBER(C.IVTG_PRGS_STCD ||'1')
                ELSE TO_NUMBER(C.IVTG_PRGS_STCD||'2')
            END AS SER2 /* 순서2 */
      FROM CFNC_IVAP_L E
      LEFT OUTER JOIN CFNC_IVTG_M C   /* 심사기본 */
        ON E.MBCM_NO       = C.MBCM_NO
       AND E.LOAN_NO       = C.LOAN_NO
      LEFT OUTER JOIN CFNC_IVPG_L D   /* 심사진행내역 */
        ON E.MBCM_NO       = D.MBCM_NO
       AND E.LOAN_NO       = D.LOAN_NO
       AND E.IVTG_PRGS_SEQ = D.IVTG_PRGS_SEQ
         , CFNC_LOAN_M A        /* 대출기본 */
         , CFNC_BGDS_I J    /* FNC_매출상품정보 */
     WHERE E.MBCM_NO = A.MBCM_NO
       AND E.LOAN_NO = A.LOAN_NO
       AND A.AGRE_CD  = J.AGRE_CD
       AND A.LNAL_ACTC_DT >= J.GDS_APLY_STDT
       AND A.LNAL_ACTC_DT <= J.GDS_APLY_ENDT
       AND E.MBCM_NO = '855'
       AND E.APRVL_DMAN_DT BETWEEN  TO_CHAR(TRUNC(SYSDATE) -7, 'YYYYMMDD') AND TO_CHAR(TRUNC(SYSDATE) , 'YYYYMMDD')
       AND E.INS_FNNC_APRVL_FIN_DT   BETWEEN TO_CHAR(TRUNC(SYSDATE) , 'YYYYMMDD') AND TO_CHAR(TRUNC(SYSDATE) , 'YYYYMMDD')
       AND A.SALE_GDS_CD <> 'A701'
       AND A.CANC_YN          <> 'Y'
       AND A.DEL_YN           <> 'Y'
       AND E.APRVL_KNCD       = '1'
       AND E.APRVL_STCD       <> '05'
       AND (E.IVTG_PRGS_SEQ, E.IVTG_APRVL_SEQ) IN ( SELECT MAX(IVTG_PRGS_SEQ), MAX(IVTG_APRVL_SEQ)
                                                      FROM CFNC_IVAP_L
                                                     WHERE MBCM_NO = E.MBCM_NO
                                                       AND LOAN_NO = E.LOAN_NO
                                                       AND IVTG_PRGS_SEQ = ( SELECT MAX(IVTG_PRGS_SEQ) 
                                                                               FROM CFNC_IVAP_L
                                                                              WHERE MBCM_NO = E.MBCM_NO
                                                                                AND LOAN_NO = E.LOAN_NO) )
     ORDER  BY SER2
         , D.FRST_REG_DT DESC
         , D.FRST_REG_TIME DESC
)
SELECT 
       A.APRVL_DMAN_DT AS "Requested date" 
     , A.IVTG_PRGS_STCD AS "Evaluation status"
     , A.BFHD_ACTC_APPC_NO AS "Pre-Application No"	
     , A.LOAN_NO  AS "Loan No."
     , R.RNNO         AS "Customer ID No."
     , A.CSTNO||' : '||N.CUST_NM AS "Customer Name"
     , A.SALE_GDS_CD||' : '||A.LOAN_GDS_MCCD||' : '||A.LOAN_GDS_SCCD AS "Product Code"
     , A.AGRE_CD||' : '||G.STND_CD_VAL_DESC AS "Project Code"	
     , A.LNAM AS "Loan Amt."
     , A.FSTK_APRV_YN	AS "Fast Approval"
     , F.CMCP_IVTG_RSLT_CD AS "CSS result"
     , A.INS_FNNC_IVTG_RSLT_CD	AS "Final Result"
     , A.IVTG_FIN_DT AS "Evaluatino Date"
     , A.IVER_USER_NO||' : '|| K.USER_NM 	AS "Evaluator"
     , A.IVTG_APRVL_RSLT_CD AS "Approval result"
     , A.INS_FNNC_APRVL_FIN_DT AS "Approval Date"
     , A.APOF_USER_NO||DECODE(A.APOF_USER_NO,' ','', ' : ')||L.USER_NM 	AS	"Approver" 
  FROM CFNC_IVAP_W A
  LEFT OUTER JOIN CFNC_CMIV_L F   /* 전산심사내역 */
    ON A.MBCM_NO = F.MBCM_NO
   AND A.LOAN_NO = F.CMCP_IVTG_TRGT_NO
  LEFT OUTER JOIN CCMM_CODE_D G   /* CMM_표준코드상세 - 프로젝트코드 */
    ON A.AGRE_CD    = G.STND_CD_VAL
   AND G.STND_CD_NM = 'AGRE_CD'
  LEFT OUTER JOIN CCMM_CODE_D H   /* CMM_표준코드상세 - 프로모션코드 */
    ON A.PROMT_CD   = H.STND_CD_VAL
   AND H.STND_CD_NM = 'PROMT_CD'
  LEFT OUTER JOIN CFNC_LNVH_I I    /* CNFC_대출차량정보 */
    ON I.MBCM_NO       = A.MBCM_NO
   AND I.LOAN_NO       = A.LOAN_NO
   AND I.LOAN_VHCL_SEQ = '1'
  LEFT OUTER JOIN CFNC_MLPR_L  M  /* FNC_모바일론프로모션관리 */
    ON A.AGRE_CD      = M.AGRE_CD       
   AND A.MBL_PROMT_CD = M.PROMT_CD   
   AND I.ATMB_MODL_CD = M.MODL_CD
   AND A.LNAL_ACTC_DT BETWEEN M.GDS_APLY_STDT AND M.GDS_APLY_ENDT
  LEFT OUTER JOIN CCMM_USER_M K   /* CMM_사용자기본 - 심사자명 */
    ON A.MBCM_NO      = K.MBCM_NO
   AND A.IVER_USER_NO = K.USER_NO
  LEFT OUTER JOIN CCMM_USER_M L   /* CMM_사용자기본 - 결재자명 */
    ON A.MBCM_NO      = L.MBCM_NO
   AND A.APOF_USER_NO = L.USER_NO
  LEFT OUTER JOIN CCST_CSNM_L N /* CST_고객명기본 - 고객명 */
    ON A.MBCM_NO = N.MBCM_NO
   AND A.CSTNO = N.CSTNO
   AND N.CUST_NM_DVCD = '1' /* 태국명으로 조회(기업 고객 없음) */
     , CCST_RNNO_M R /* CST_고객실명기본 */
 WHERE 1 = 1
   AND A.MBCM_NO        = R.MBCM_NO
   AND A.CSTNO          = R.CSTNO