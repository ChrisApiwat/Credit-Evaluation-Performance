# -*- coding: utf-8 -*-
import jaydebeapi
import pandas as pd
import os
from datetime import datetime
import schedule
import time

jdbc_driver = "com.tmax.tibero.jdbc.TbDriver"
jdbc_url = "jdbc:tibero:thin:@192.169.10.51:18629:DSTFCC"  
db_user = "Apiwat"  #
db_password = "Apiw@2024"  
jdbc_jar = "tibero6-jdbc.jar"   

# »ÃÐ¡ÒÈ¿Ñ§¡ìªÑ¹ query_data()
def query_data():
    try:
        # àª×èÍÁµèÍ°Ò¹¢éÍÁÙÅ
        conn = jaydebeapi.connect(jdbc_driver, jdbc_url, [db_user, db_password], jdbc_jar)
        
        # ÊÃéÒ§ cursor
        cursor = conn.cursor()

        # SQL1 Query GIS 04001 Pending
        query1 = """ 
        /* card/cor/fnc/ivtg/ivtgbsicebi/IvtgBsic-retvLstIvtgBsic */
WITH CFNC_IVTG_W AS (
    SELECT /*+ INDEX(A IX_CFNC_IVTG_M_01) INDEX(B PK_CFNC_LOAN_M) INDEX(G PK_CFNC_BGDS_I) INDEX(F PK_CFNC_IVPG_L) */
           A.MBCM_NO                  /* ????? */ 
         , A.LOAN_NO                  /* ???? */
         , A.IVTG_DMAN_DT             /* ?????? */
         , A.IVTG_PRGS_STCD           /* ???????? */
         , A.LAST_IVTG_PRGS_SEQ       /* ???????? */
         , A.XCPT_APRV_DMAN_DT        /* ???????? */
         , A.XCPT_APRV_DMAN_YN        /* ???????? */
         , A.IVTG_BRCD                /* ?????? */
         , A.IVER_USER_NO             /* ???????? */
         , B.CSTNO                    /* ???? */
         , B.IDVD_CRPT_DVCD           /* ???????? */
         , CASE WHEN B.LOAN_PRGS_STEP_CD = '20' AND A.IVTG_PRGS_STCD = '99' THEN '21'
                WHEN B.LOAN_PRGS_STEP_CD = '20' AND A.IVTG_PRGS_STCD = '97' THEN '22'
                ELSE B.LOAN_PRGS_STEP_CD
           END AS LOAN_PRGS_STEP_CD   /* ???????? */
         , B.SALE_GDS_CD              /* ?????? */
         , G.LOAN_GDS_LCCD            /* ????????? */
         , G.LOAN_GDS_MCCD            /* ????????? */
         , G.LOAN_GDS_SCCD            /* ????????? */
         , B.AGRE_CD                  /* ???? */
         , B.LNAM                     /* ???? */
         , B.LOAN_TOT_FEE_AMT         /* ???????? */
         , B.LOAN_BRCD                /* ?????? */
         , B.LOAN_DT                  /* ???? */
         , B.FRST_REG_DT              /* ?????? */
         , B.FRST_REG_TIME            /* ?????? */
         , B.FRST_REG_USER_NO         /* ????????? */
         , B.BFHD_ACTC_APPC_NO        /* ???????? */
         , B.OBD_CHNL_DVCD            /* ????????? */
         , B.FRM_DT                   /* ???? */
         , B.LNAL_ACTC_DT             /* ???????? */
         , F.IVTG_FIN_DT              /* ?????? */
         , F.INS_FNNC_IVTG_RSLT_CD    /* ?????????? */
         , DECODE(F.INS_FNNC_IVTG_RSLT_CD, '04', F.IVTG_CANC_RSCD, NULL)  AS IVTG_DENL_RSCD   /* ???????? */
         , DECODE(F.INS_FNNC_IVTG_RSLT_CD, '05', F.IVTG_CANC_RSCD, NULL)  AS IVTG_RTRC_RSCD   /* ???????? */
         , DECODE(F.INS_FNNC_IVTG_RSLT_CD, '06', F.IVTG_CANC_RSCD, NULL)  AS IVTG_CANC_RSCD   /* ???????? */
         , DECODE(F.INS_FNNC_IVTG_RSLT_CD, '07', F.IVTG_CANC_RSCD, NULL)  AS IVTG_WAIT_RSCD   /* ???????? */
         , G.FSTK_APRV_YN             /* FastTrack???? */
      FROM CFNC_IVTG_M A   /* FNC_???? */
      JOIN CFNC_LOAN_M B   /* FNC_???? */
        ON A.MBCM_NO = B.MBCM_NO
       AND A.LOAN_NO = B.LOAN_NO
      JOIN CFNC_BGDS_I G   /* FNC_?????? */
        ON B.AGRE_CD = G.AGRE_CD
       AND B.LNAL_ACTC_DT BETWEEN G.GDS_APLY_STDT AND G.GDS_APLY_ENDT
      LEFT OUTER JOIN CFNC_IVPG_L F   /* FNC_?????? */
        ON A.MBCM_NO = F.MBCM_NO
       AND A.LOAN_NO = F.LOAN_NO
       AND A.LAST_IVTG_PRGS_SEQ = F.IVTG_PRGS_SEQ
     WHERE 1=1
       AND A.MBCM_NO = '855'
       AND A.IVTG_DMAN_DT   BETWEEN TO_CHAR(TRUNC(SYSDATE) - 15, 'YYYYMMDD')  AND TO_CHAR(TRUNC(SYSDATE), 'YYYYMMDD') 
       AND B.SALE_GDS_CD       <> 'A701'
       AND B.CANC_YN           <> 'Y'
       AND B.DEL_YN            <> 'Y'
       AND B.LOAN_PRGS_STEP_CD IN ('20', '29', '30', '40', '41')  /* 20:??,29:????,30:????,40:??,41:???? */
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
  LEFT OUTER JOIN CCMM_CODE_D C   /* CMM_?????? - ?????? */
    ON T.AGRE_CD = C.STND_CD_VAL
   AND C.STND_CD_NM = 'AGRE_CD'
  LEFT OUTER JOIN CCMM_USER_M H   /* CMM_????? */
    ON T.MBCM_NO = H.MBCM_NO
   AND T.IVER_USER_NO = H.USER_NO
  LEFT OUTER JOIN CFNC_CMIV_L J   /* ?????? */
    ON T.MBCM_NO = J.MBCM_NO
   AND T.LOAN_NO = J.CMCP_IVTG_TRGT_NO 
  LEFT OUTER JOIN CCST_CSNM_L K   /* ????? */
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
  JOIN CCST_RNNO_M R   /* CST_?????? */
    ON T.MBCM_NO = R.MBCM_NO
   AND T.CSTNO = R.CSTNO
        """
        # SQL2 Query GIS 04003 Approval
        query2 = """
        /* card/cor/fnc/ivtg/ivtgaprvlprgshistebi/IvtgAprvlPrgsHist-retvLstIvtgAprvlTrgt */
WITH CFNC_IVAP_W AS (
    SELECT /*+ INDEX (E IX_CFNC_IVAP_L_01) INDEX (A PK_CFNC_LOAN_M) */
           A.MBCM_NO                        /* ????? */
         , A.LOAN_NO                        /* ???? */
         , A.CSTNO                          /* ???? */
         , A.PYT_DMAN_NO                    /* ?????? */
         , A.BFHD_ACTC_APPC_NO              /* ???????? */
         , A.IDVD_CRPT_DVCD                 /* ???????? */
         , A.CSGR_DVCD                      /* ???????? */
         , A.LNAL_ACTC_DT                   /* ???????? */
         , A.LNAL_ACTC_SEQ                  /* ???????? */
         , A.INS_LEAS_DVCD                  /* ???????? */
         , A.SALE_GDS_CD                    /* ?????? */
         , A.CPST_DVCD                      /* ??????? */
         , A.AGRE_CD                        /* ???? */
         , A.PROMT_CD                       /* ?????? */
         , A.MBL_PROMT_CD                   /* ????????? */
         , A.CPST_NO                         /* ????? */
         , A.LOAN_TRM_MCNT                   /* ??????? */
         , A.DFRM_TRM_MCNT                   /* ??????? */
         , A.EFEC_IRATE                      /* ????? */
         , A.FLAT_IRATE                      /* FLAT??? */
         , A.LNAM                            /* ???? */
         , A.TINT_AMT                        /* ????? */
         , A.MM_PYMN_AMT                     /* ????? */
         , A.RPRS_REPAY_MTCD                 /* ???????? */
         , A.REPAY_TYCD                      /* ?????? */
         , A.LOAN_BRCD                       /* ?????? */
         , A.LOAN_DT                         /* ???? */
         , A.EXPR_DT                         /* ???? */
         , A.FRM_DT                          /* ???? */
         , A.SETL_DD                         /* ??? */
         , A.FRST_SETL_DT                    /* ?????? */
         , A.LSTL_DT                         /* ?????? */
         , A.LAST_DMND_NTH                   /* ?????? */
         , A.LSRC_DT                         /* ?????? */
         , A.LOAN_TOT_FEE_AMT                /* ???????? */
         , A.PPAY_AMT                        /* ???? */
         , A.PRCV_RATE                       /* ??? */
         , A.SSPN_AMT                        /* ???? */
         , A.SSPN_RATE                       /* ??? */
         , A.POCP_INS_AMT                    /* ?????? */
         , A.LAST_MNRC_AMT                   /* ?????? */
         , A.LOAN_BAL                        /* ???? */
         , A.PRCV_BAL                        /* ???? */
         , A.FLPY_YN                         /* ???? */
         , A.FLPY_DT                         /* ???? */
         , A.DTST_ACCP_MTCD                  /* ????????? */
         , A.DTST_ADDR                       /* ????? */
         , CASE WHEN A.LOAN_PRGS_STEP_CD = '20' AND C.IVTG_PRGS_STCD = '99' THEN '21'
                WHEN A.LOAN_PRGS_STEP_CD = '20' AND C.IVTG_PRGS_STCD = '97' THEN '22'
                ELSE A.LOAN_PRGS_STEP_CD
            END AS LOAN_PRGS_STEP_CD /* ???????? */
         , A.CANC_YN                         /* ???? */
         , A.CANC_DT                         /* ???? */
         , A.LOAN_CANC_MEMO_CTNT             /* ???????? */
         , A.DEL_YN                          /* ???? */
         , A.LOAN_PRPS_RSCD                  /* ???????? */
         , C.IVTG_DMAN_DT                    /* ?????? */
         , C.IVTG_PRGS_STCD                  /* ???????? */
         , C.LAST_IVTG_PRGS_SEQ              /* ???????? */
         , C.XCPT_APRV_DMAN_DT               /* ???????? */
         , C.XCPT_APRV_DMAN_YN               /* ???????? */
         , C.IVTG_JBTY_CD                    /* ?????? */
         , C.IVTG_PSTN_CD                    /* ?????? */
         , C.DBTR_IDNO                       /* ??????? */
         , C.IVTG_BRCD                       /* ?????? */
         , C.IVER_USER_NO                    /* ???????? */
         , C.BROF_OPN_CTNT                   /* ?????? */
         , D.IVTG_PRGS_SEQ                   /* ?????? */
         , D.IVTG_DMAN_CHGR_USER_NO          /* ???????????? */
         , D.IVER_AOCT_DT                    /* ??????? */
         , D.IVTG_STEP_CD                    /* ?????? */
         , D.IVTG_FIN_DT                     /* ?????? */
         , D.LAST_APRVL_STEP_CD              /* ???????? */
         , D.INS_FNNC_IVTG_RSLT_CD           /* ?????????? */
         , D.IVTG_LVOT_RSCD                  /* ???????? */
         , E.IVTG_APRVL_SEQ                  /* ?????? */
         , E.APRVL_DMAN_DT                   /* ?????? */
         , E.APRVL_STEP_CD                   /* ?????? */
         , E.APRVL_STCD                      /* ?????? */
         , E.APRVL_KNCD                      /* ?????? */
         , E.INS_FNNC_APRVL_FIN_DT        /* ?????????? */
         , E.APOF_USER_NO                    /* ???????? */
         , E.IVTG_APRVL_RSLT_CD              /* ???????? */
         , E.APRVL_OPN_CTNT                  /* ?????? */
         , J.FSTK_APRV_YN                    /* FastTrack???? */
         , J.LOAN_GDS_LCCD                   /* ????????? */
         , J.LOAN_GDS_MCCD                   /* ????????? */
         , J.LOAN_GDS_SCCD                   /* ????????? */
         , CASE WHEN C.IVTG_PRGS_STCD IN ('00:Confirmed', '97', '99:Rejected') THEN 1001
                WHEN J.FSTK_APRV_YN = 'Y'                   THEN TO_NUMBER(C.IVTG_PRGS_STCD ||'1')
                ELSE TO_NUMBER(C.IVTG_PRGS_STCD||'2')
            END AS SER2 /* ??2 */
      FROM CFNC_IVAP_L E
      LEFT OUTER JOIN CFNC_IVTG_M C   /* ???? */
        ON E.MBCM_NO       = C.MBCM_NO
       AND E.LOAN_NO       = C.LOAN_NO
      LEFT OUTER JOIN CFNC_IVPG_L D   /* ?????? */
        ON E.MBCM_NO       = D.MBCM_NO
       AND E.LOAN_NO       = D.LOAN_NO
       AND E.IVTG_PRGS_SEQ = D.IVTG_PRGS_SEQ
         , CFNC_LOAN_M A        /* ???? */
         , CFNC_BGDS_I J    /* FNC_?????? */
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
  LEFT OUTER JOIN CFNC_CMIV_L F   /* ?????? */
    ON A.MBCM_NO = F.MBCM_NO
   AND A.LOAN_NO = F.CMCP_IVTG_TRGT_NO
  LEFT OUTER JOIN CCMM_CODE_D G   /* CMM_?????? - ?????? */
    ON A.AGRE_CD    = G.STND_CD_VAL
   AND G.STND_CD_NM = 'AGRE_CD'
  LEFT OUTER JOIN CCMM_CODE_D H   /* CMM_?????? - ?????? */
    ON A.PROMT_CD   = H.STND_CD_VAL
   AND H.STND_CD_NM = 'PROMT_CD'
  LEFT OUTER JOIN CFNC_LNVH_I I    /* CNFC_?????? */
    ON I.MBCM_NO       = A.MBCM_NO
   AND I.LOAN_NO       = A.LOAN_NO
   AND I.LOAN_VHCL_SEQ = '1'
  LEFT OUTER JOIN CFNC_MLPR_L  M  /* FNC_?????????? */
    ON A.AGRE_CD      = M.AGRE_CD       
   AND A.MBL_PROMT_CD = M.PROMT_CD   
   AND I.ATMB_MODL_CD = M.MODL_CD
   AND A.LNAL_ACTC_DT BETWEEN M.GDS_APLY_STDT AND M.GDS_APLY_ENDT
  LEFT OUTER JOIN CCMM_USER_M K   /* CMM_????? - ???? */
    ON A.MBCM_NO      = K.MBCM_NO
   AND A.IVER_USER_NO = K.USER_NO
  LEFT OUTER JOIN CCMM_USER_M L   /* CMM_????? - ???? */
    ON A.MBCM_NO      = L.MBCM_NO
   AND A.APOF_USER_NO = L.USER_NO
  LEFT OUTER JOIN CCST_CSNM_L N /* CST_????? - ??? */
    ON A.MBCM_NO = N.MBCM_NO
   AND A.CSTNO = N.CSTNO
   AND N.CUST_NM_DVCD = '1' /* ????? ??(?? ?? ??) */
     , CCST_RNNO_M R /* CST_?????? */
 WHERE 1 = 1
   AND A.MBCM_NO        = R.MBCM_NO
   AND A.CSTNO          = R.CSTNO 
        """
        # SQL3 Query GIS 04001 Cancelled
        query3 = """ 
        /* card/cor/fnc/ivtg/ivtgbsicebi/IvtgBsic-retvLstIvtgBsic */
WITH CFNC_IVTG_W AS (
    SELECT /*+ INDEX(A IX_CFNC_IVTG_M_01) INDEX(B PK_CFNC_LOAN_M) INDEX(G PK_CFNC_BGDS_I) INDEX(F PK_CFNC_IVPG_L) */
           A.MBCM_NO                  /* ????? */ 
         , A.LOAN_NO                  /* ???? */
         , A.IVTG_DMAN_DT             /* ?????? */
         , A.IVTG_PRGS_STCD           /* ???????? */
         , A.LAST_IVTG_PRGS_SEQ       /* ???????? */
         , A.XCPT_APRV_DMAN_DT        /* ???????? */
         , A.XCPT_APRV_DMAN_YN        /* ???????? */
         , A.IVTG_BRCD                /* ?????? */
         , A.IVER_USER_NO             /* ???????? */
         , B.CSTNO                    /* ???? */
         , B.IDVD_CRPT_DVCD           /* ???????? */
         , CASE WHEN B.LOAN_PRGS_STEP_CD = '20' AND A.IVTG_PRGS_STCD = '99' THEN '21'
                WHEN B.LOAN_PRGS_STEP_CD = '20' AND A.IVTG_PRGS_STCD = '97' THEN '22'
                ELSE B.LOAN_PRGS_STEP_CD
           END AS LOAN_PRGS_STEP_CD   /* ???????? */
         , B.SALE_GDS_CD              /* ?????? */
         , G.LOAN_GDS_LCCD            /* ????????? */
         , G.LOAN_GDS_MCCD            /* ????????? */
         , G.LOAN_GDS_SCCD            /* ????????? */
         , B.AGRE_CD                  /* ???? */
         , B.LNAM                     /* ???? */
         , B.LOAN_TOT_FEE_AMT         /* ???????? */
         , B.LOAN_BRCD                /* ?????? */
         , B.LOAN_DT                  /* ???? */
         , B.FRST_REG_DT              /* ?????? */
         , B.FRST_REG_TIME            /* ?????? */
         , B.FRST_REG_USER_NO         /* ????????? */
         , B.BFHD_ACTC_APPC_NO        /* ???????? */
         , B.OBD_CHNL_DVCD            /* ????????? */
         , B.FRM_DT                   /* ???? */
         , B.LNAL_ACTC_DT             /* ???????? */
         , F.IVTG_FIN_DT              /* ?????? */
         , F.INS_FNNC_IVTG_RSLT_CD    /* ?????????? */
         , DECODE(F.INS_FNNC_IVTG_RSLT_CD, '04', F.IVTG_CANC_RSCD, NULL)  AS IVTG_DENL_RSCD   /* ???????? */
         , DECODE(F.INS_FNNC_IVTG_RSLT_CD, '05', F.IVTG_CANC_RSCD, NULL)  AS IVTG_RTRC_RSCD   /* ???????? */
         , DECODE(F.INS_FNNC_IVTG_RSLT_CD, '06', F.IVTG_CANC_RSCD, NULL)  AS IVTG_CANC_RSCD   /* ???????? */
         , DECODE(F.INS_FNNC_IVTG_RSLT_CD, '07', F.IVTG_CANC_RSCD, NULL)  AS IVTG_WAIT_RSCD   /* ???????? */
         , G.FSTK_APRV_YN             /* FastTrack???? */
      FROM CFNC_IVTG_M A   /* FNC_???? */
      JOIN CFNC_LOAN_M B   /* FNC_???? */
        ON A.MBCM_NO = B.MBCM_NO
       AND A.LOAN_NO = B.LOAN_NO
      JOIN CFNC_BGDS_I G   /* FNC_?????? */
        ON B.AGRE_CD = G.AGRE_CD
       AND B.LNAL_ACTC_DT BETWEEN G.GDS_APLY_STDT AND G.GDS_APLY_ENDT
      LEFT OUTER JOIN CFNC_IVPG_L F   /* FNC_?????? */
        ON A.MBCM_NO = F.MBCM_NO
       AND A.LOAN_NO = F.LOAN_NO
       AND A.LAST_IVTG_PRGS_SEQ = F.IVTG_PRGS_SEQ
     WHERE 1=1
       AND A.MBCM_NO = '855'
       AND A.IVTG_PRGS_STCD ='97'
       AND A.IVTG_DMAN_DT   = TO_CHAR(TRUNC(SYSDATE) , 'YYYYMMDD')
       AND B.SALE_GDS_CD       <> 'A701'
       AND B.CANC_YN           <> 'Y'
       AND B.DEL_YN            <> 'Y'
       AND B.LOAN_PRGS_STEP_CD IN ('20', '29', '30', '40', '41')  /* 20:??,29:????,30:????,40:??,41:???? */
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
  LEFT OUTER JOIN CCMM_CODE_D C   /* CMM_?????? - ?????? */
    ON T.AGRE_CD = C.STND_CD_VAL
   AND C.STND_CD_NM = 'AGRE_CD'
  LEFT OUTER JOIN CCMM_USER_M H   /* CMM_????? */
    ON T.MBCM_NO = H.MBCM_NO
   AND T.IVER_USER_NO = H.USER_NO
  LEFT OUTER JOIN CFNC_CMIV_L J   /* ?????? */
    ON T.MBCM_NO = J.MBCM_NO
   AND T.LOAN_NO = J.CMCP_IVTG_TRGT_NO 
  LEFT OUTER JOIN CCST_CSNM_L K   /* ????? */
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
  JOIN CCST_RNNO_M R   /* CST_?????? */
    ON T.MBCM_NO = R.MBCM_NO
   AND T.CSTNO = R.CSTNO
        """
        # 04001 Timestamp
        query4 = """ /* card/cor/fnc/ivtg/ivtgbsicebi/IvtgBsic-retvLstIvtgBsic */
WITH CFNC_IVTG_W AS (
    SELECT /*+ INDEX(A IX_CFNC_IVTG_M_01) INDEX(B PK_CFNC_LOAN_M) INDEX(G PK_CFNC_BGDS_I) INDEX(F PK_CFNC_IVPG_L) */
           A.MBCM_NO                  /* ????? */ 
         , A.LOAN_NO                  /* ???? */
         , A.IVTG_DMAN_DT             /* ?????? */
         , A.IVTG_PRGS_STCD           /* ???????? */
         , A.LAST_IVTG_PRGS_SEQ       /* ???????? */
         , A.XCPT_APRV_DMAN_DT        /* ???????? */
         , A.XCPT_APRV_DMAN_YN        /* ???????? */
         , A.IVTG_BRCD                /* ?????? */
         , A.IVER_USER_NO             /* ???????? */
         , B.CSTNO                    /* ???? */
         , B.IDVD_CRPT_DVCD           /* ???????? */
         , CASE WHEN B.LOAN_PRGS_STEP_CD = '20' AND A.IVTG_PRGS_STCD = '99' THEN '21'
                WHEN B.LOAN_PRGS_STEP_CD = '20' AND A.IVTG_PRGS_STCD = '97' THEN '22'
                ELSE B.LOAN_PRGS_STEP_CD
           END AS LOAN_PRGS_STEP_CD   /* ???????? */
         , B.SALE_GDS_CD              /* ?????? */
         , G.LOAN_GDS_LCCD            /* ????????? */
         , G.LOAN_GDS_MCCD            /* ????????? */
         , G.LOAN_GDS_SCCD            /* ????????? */
         , B.AGRE_CD                  /* ???? */
         , B.LNAM                     /* ???? */
         , B.LOAN_TOT_FEE_AMT         /* ???????? */
         , B.LOAN_BRCD                /* ?????? */
         , B.LOAN_DT                  /* ???? */
         , B.FRST_REG_DT              /* ?????? */
         , B.FRST_REG_TIME            /* ?????? */
         , B.FRST_REG_USER_NO         /* ????????? */
         , B.BFHD_ACTC_APPC_NO        /* ???????? */
         , B.OBD_CHNL_DVCD            /* ????????? */
         , B.FRM_DT                   /* ???? */
         , B.LNAL_ACTC_DT             /* ???????? */
         , F.IVTG_FIN_DT              /* ?????? */
         , F.INS_FNNC_IVTG_RSLT_CD    /* ?????????? */
         , DECODE(F.INS_FNNC_IVTG_RSLT_CD, '04', F.IVTG_CANC_RSCD, NULL)  AS IVTG_DENL_RSCD   /* ???????? */
         , DECODE(F.INS_FNNC_IVTG_RSLT_CD, '05', F.IVTG_CANC_RSCD, NULL)  AS IVTG_RTRC_RSCD   /* ???????? */
         , DECODE(F.INS_FNNC_IVTG_RSLT_CD, '06', F.IVTG_CANC_RSCD, NULL)  AS IVTG_CANC_RSCD   /* ???????? */
         , DECODE(F.INS_FNNC_IVTG_RSLT_CD, '07', F.IVTG_CANC_RSCD, NULL)  AS IVTG_WAIT_RSCD   /* ???????? */
         , G.FSTK_APRV_YN             /* FastTrack???? */
      FROM CFNC_IVTG_M A   /* FNC_???? */
      JOIN CFNC_LOAN_M B   /* FNC_???? */
        ON A.MBCM_NO = B.MBCM_NO
       AND A.LOAN_NO = B.LOAN_NO
      JOIN CFNC_BGDS_I G   /* FNC_?????? */
        ON B.AGRE_CD = G.AGRE_CD
       AND B.LNAL_ACTC_DT BETWEEN G.GDS_APLY_STDT AND G.GDS_APLY_ENDT
      LEFT OUTER JOIN CFNC_IVPG_L F   /* FNC_?????? */
        ON A.MBCM_NO = F.MBCM_NO
       AND A.LOAN_NO = F.LOAN_NO
       AND A.LAST_IVTG_PRGS_SEQ = F.IVTG_PRGS_SEQ
     WHERE 1=1
       AND A.MBCM_NO = '855'
       AND A.IVTG_DMAN_DT  = TO_CHAR(TRUNC(SYSDATE), 'YYYYMMDD')
       AND B.SALE_GDS_CD       <> 'A701'
       AND B.CANC_YN           <> 'Y'
       AND B.DEL_YN            <> 'Y'
       AND B.LOAN_PRGS_STEP_CD IN ('20', '29', '30', '40', '41')  /* 20:??,29:????,30:????,40:??,41:???? */
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
  LEFT OUTER JOIN CCMM_CODE_D C   /* CMM_?????? - ?????? */
    ON T.AGRE_CD = C.STND_CD_VAL
   AND C.STND_CD_NM = 'AGRE_CD'
  LEFT OUTER JOIN CCMM_USER_M H   /* CMM_????? */
    ON T.MBCM_NO = H.MBCM_NO
   AND T.IVER_USER_NO = H.USER_NO
  LEFT OUTER JOIN CFNC_CMIV_L J   /* ?????? */
    ON T.MBCM_NO = J.MBCM_NO
   AND T.LOAN_NO = J.CMCP_IVTG_TRGT_NO 
  LEFT OUTER JOIN CCST_CSNM_L K   /* ????? */
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
  JOIN CCST_RNNO_M R   /* CST_?????? */
    ON T.MBCM_NO = R.MBCM_NO
   AND T.CSTNO = R.CSTNO """
        
        
        # ´Ö§¢éÍÁÙÅ¨Ò¡°Ò¹¢éÍÁÙÅ
        cursor.execute(query1)
        data1 = cursor.fetchall()
        cursor.execute(query2)
        data2 = cursor.fetchall()
        cursor.execute(query3)
        data3 = cursor.fetchall()
        cursor.execute(query4)
        data4 = cursor.fetchall()

        # á»Å§¢éÍÁÙÅà»ç¹ DataFrame
        df1 = pd.DataFrame(data1, columns=['Evaluator', 'On-boarding Channel', 'Product Code', 'Requested date', 'Request time', 'Customer No', 'Customer Name', 'Customer ID No.', 'Loan No.', 'Evaluating progress status', 'Project Code', 'Loan Amt.', 'Pre-Application No', 'CSS evaluation result', 'Final Result', 'Return reason', 'Cancellation Reason', 'Decline reason', 'Pending Reason', 'Evaluation Date', 'Confirmation date'])
        df2 = pd.DataFrame(data2, columns=['Requested date', 'Evaluation status', 'Pre-Application No', 'Loan No.', 'Customer ID No.', 'Customer Name', 'Product Code', 'Project Code', 'Loan Amt.', 'Fast Approval', 'CSS result', 'Final Result', 'Evaluation Date', 'Evaluator', 'Approval result', 'Approval Date', 'Approver'])
        df3 = pd.DataFrame(data3, columns=['Evaluator', 'On-boarding Channel', 'Product Code', 'Requested date', 'Request time', 'Customer No', 'Customer Name', 'Customer ID No.', 'Loan No.', 'Evaluating progress status', 'Project Code', 'Loan Amt.', 'Pre-Application No', 'CSS evaluation result', 'Final Result', 'Return reason', 'Cancellation Reason', 'Decline reason', 'Pending Reason', 'Evaluation Date', 'Confirmation date'])
        df4 = pd.DataFrame(data4, columns=['Evaluator', 'On-boarding Channel', 'Product Code', 'Requested date', 'Request time', 'Customer No', 'Customer Name', 'Customer ID No.', 'Loan No.', 'Evaluating progress status', 'Project Code', 'Loan Amt.', 'Pre-Application No', 'CSS evaluation result', 'Final Result', 'Return reason', 'Cancellation Reason', 'Decline reason', 'Pending Reason', 'Evaluation Date', 'Confirmation date'])

        # ¿Ñ§¡ìªÑ¹á»Å§¢éÍÁÙÅ
        def some_transformation_function(df):
            df['Requested date'] = pd.to_datetime(df['Requested date'], errors='coerce')
            df['Loan No.'] = df['Loan No.'].astype(str)

            # µÃÇ¨ÊÍº»ÃÐàÀ·¤ÍÅÑÁ¹ìà¾×èÍ»éÍ§¡Ñ¹ FutureWarning
            if 'Requested date' in df.columns:
                df['Requested date'] = df['Requested date'].astype('datetime64[ns]')

            return df

        # Apply transformation
        transformed_df1 = some_transformation_function(df1)
        transformed_df2 = some_transformation_function(df2)
        transformed_df3 = some_transformation_function(df3)
        transformed_df4 = some_transformation_function(df4)
        

        # µÑé§ª×èÍä¿ÅìµÒÁàÇÅÒ
        current_time = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
        file1 = f"Evaluation_List_{current_time}.xlsx"
        file2 = f"Evaluation_Approval_{current_time}.xlsx"
        file3 = f"Evaluation_List_Cancelled_{current_time}.xlsx"
        file4 = f"Evaluation_Timestamp_{current_time}.xlsx"

        # Åºä¿Åìà¡èÒ
        for file in os.listdir():
            if file.startswith("Evaluation_") and file.endswith(".xlsx"):
                os.remove(file)

        # ºÑ¹·Ö¡à»ç¹ Excel
        transformed_df1.to_excel(file1, index=False, engine="openpyxl")
        transformed_df2.to_excel(file2, index=False, engine="openpyxl")
        transformed_df3.to_excel(file3, index=False, engine="openpyxl")
        transformed_df4.to_excel(file4, index=False, engine="openpyxl")

        print("Data has been transformed and saved to Excel.")

    except Exception as e:
        print(f"Error occurred: {e}")

    finally:
        if 'conn' in locals():  # àªç¤ÇèÒ conn ¶Ù¡»ÃÐ¡ÒÈ¡èÍ¹¨Ð»Ô´
            conn.close()

# ¿Ñ§¡ìªÑ¹ÂéÒÂä¿Åìä» backup
def move_to_backup():
    backup_dir = "backup"
    os.makedirs(backup_dir, exist_ok=True)

    for file in os.listdir():
        if file.startswith("Evaluation_") and file.endswith(".xlsx"):
            backup_path = os.path.join(backup_dir, file)
            if not os.path.exists(backup_path):
                os.rename(file, backup_path)
    print("Files have been moved to backup.")
    
# µÑé§àÇÅÒÃÑ¹à©¾ÒÐÇÑ¹¨Ñ¹·Ãì-ÈØ¡Ãì (08:30 - 21:30)
for hour in ["09:00", "11:00", "13:00","14:23", "15:30", "17:00", "20:40"]:
    schedule.every().monday.at(hour).do(query_data)
    schedule.every().tuesday.at(hour).do(query_data)
    schedule.every().wednesday.at(hour).do(query_data)
    schedule.every().thursday.at(hour).do(query_data)
    schedule.every().friday.at(hour).do(query_data)

# µÑé§àÇÅÒÃÑ¹à©¾ÒÐÇÑ¹àÊÒÃì-ÍÒ·ÔµÂì (08:30 - 21:30)
for hour in ["13:00", "15:30", "17:00", "20:40"]:
    schedule.every().saturday.at(hour).do(query_data)
    schedule.every().sunday.at(hour).do(query_data)
    
    # µÑé§àÇÅÒÂéÒÂä¿Åìä» backup àÇÅÒ 20:58 ·Ø¡ÇÑ¹
schedule.every().day.at("20:55").do(move_to_backup)

# Loop ÃÑ¹à©¾ÒÐªèÇ§àÇÅÒ 08:30 - 21:30
while True:
    now = datetime.now().strftime("%H:%M")
    if "08:30" <= now <= "21:30":
        schedule.run_pending()
    time.sleep(30)  # Å´¡ÒÃãªé CPU â´ÂÃÑ¹·Ø¡ 30 ÇÔ¹Ò·Õ
