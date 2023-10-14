using DataDictionary.Entities;
using hostingDictionary = DataDictionary.Hosting;
using DataDictionary.Utility.Results;
using DataDictionaryWebApi.Configuration;
using DevExtreme.AspNet.Data;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Newtonsoft.Json;
using Novell.Directory.Ldap;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using DataDictionary.Models.ResponseModels;

namespace DataDictionaryWebApi.Models
{

    public class PhysicalDataMixingWorkflow : IPhysicalDataMixingContract
    {
        private EahouseContext _EahouseContext;
        private IHostingEnvironment _environment;
        public PhysicalDataMixingWorkflow(EahouseContext context, IHostingEnvironment environment)
        {
            _EahouseContext = context;
            _environment = environment;
        }
        public List<PhysicalDataMixingMasterRequestModel> GetDistinctPhysical()
        {

            var data = (from s in _EahouseContext.PhysicalDataMixingMaster
                        join pddMap in _EahouseContext.DataDictionaryStatus
                         on s.IsApproved equals pddMap.ApprovalStatusId
                        join mix in _EahouseContext.DataMixingMaster
                        on s.DataMixingMasterId equals mix.DataMixingMasterId
                        where s.IsActive == true

                        select new PhysicalDataMixingMasterRequestModel
                        {
                            PhysicalDataMixingMasterId = s.PhysicalDataMixingMasterId,
                            ApprovalStatusName = pddMap.ApprovalStatusName,
                            PhysicalDataMixingVersion = s.PhysicalDataMixingVersion,
                            PhysicalDataMixingVersionDescription = s.PhysicalDataMixingVersionDescription,
                            FromDate = s.FromDate,
                            ExcelLink = s.ExcelLink,
                            ApprovalMailLink = s.ApprovalMailLink,
                            DataMixingVersion=mix.DataMixingVersion,
                            IsActive = s.IsActive
                        }).ToList();
            return data;
        }

        public void DeletePhysicalDataMixingMaster([FromBody]PhysicalDataMixingMaster newData)
        {
            try
            {
                var role = hostingDictionary.HttpContext.Current.User.Claims.ToList();
                var username = role[0].Value;
                var mainData = _EahouseContext.PhysicalDataMixingMaster.FirstOrDefault(x => x.PhysicalDataMixingMasterId == newData.PhysicalDataMixingMasterId);
                mainData.PhysicalDataMixingMasterId = mainData.PhysicalDataMixingMasterId;
                mainData.PhysicalDataMixingVersion = mainData.PhysicalDataMixingVersion;
                mainData.IsApproved = mainData.IsApproved;
                mainData.FromDate = mainData.FromDate;
                mainData.ExcelLink = mainData.ExcelLink;
                mainData.ApprovalMailLink = mainData.ApprovalMailLink;
                mainData.DataMixingMasterId = mainData.DataMixingMasterId;
                mainData.CreatedBy = mainData.CreatedBy;
                mainData.ModifiedBy = username;
                mainData.ModifiedDate = DateTime.Now;
                mainData.IsActive = false;


                _EahouseContext.PhysicalDataMixingMaster.Update(mainData);
                _EahouseContext.SaveChanges();
                var physicalDataMixingForDelete = (from s in _EahouseContext.PhysicalDataMixing
                                           where s.PhysicalDataMixingMasterId == newData.PhysicalDataMixingMasterId
                                                   select new PhysicalDataMixing
                                                   {


                                                       DataDictionaryItemId = s.DataDictionaryItemId,
                                                       DDItemServerName = s.DDItemServerName,
                                                       PhysicalDataMixingDescription = s.PhysicalDataMixingDescription,
                                                       DDItemDatabaseName = s.DDItemDatabaseName,
                                                       DDItemSchemaName = s.DDItemSchemaName,
                                                       DDItemTableName = s.DDItemTableName,
                                                       DDItemColumnName = s.DDItemColumnName,
                                                       PhysicalDataMixingRuleName = s.PhysicalDataMixingRuleName,
                                                       PhysicalDataMixingId = s.PhysicalDataMixingId,
                                                       FromDate = s.FromDate,
                                                       ThruDate = s.ThruDate,
                                                       PhysicalDataMixingMasterId = s.PhysicalDataMixingMasterId,
                                                       IsActive = s.IsActive,
                                                       CreatedBy = s.CreatedBy,
                                                       ModifiedBy = s.ModifiedBy,
                                                       ModifiedDate = s.ModifiedDate
                                                   }).ToList();
                foreach (var j in physicalDataMixingForDelete)
                {
                    var itemj = _EahouseContext.PhysicalDataMixing.FirstOrDefault(o => o.PhysicalDataMixingMasterId == j.PhysicalDataMixingMasterId && o.PhysicalDataMixingMasterId == newData.PhysicalDataMixingMasterId && o.PhysicalDataMixingId==j.PhysicalDataMixingId);

                    if (itemj != null)
                    {

                        itemj.DataDictionaryItemId = itemj.DataDictionaryItemId;
                        itemj.DDItemServerName = itemj.DDItemServerName;
                        itemj.PhysicalDataMixingDescription = itemj.PhysicalDataMixingDescription;
                        itemj.DDItemDatabaseName = itemj.DDItemDatabaseName;
                        itemj.DDItemSchemaName = itemj.DDItemSchemaName;
                        itemj.DDItemTableName = itemj.DDItemTableName;
                        itemj.DDItemColumnName = itemj.DDItemColumnName;
                        itemj.PhysicalDataMixingRuleName = itemj.PhysicalDataMixingRuleName;
                        itemj.PhysicalDataMixingId = itemj.PhysicalDataMixingId;
                        itemj.FromDate = itemj.FromDate;
                        itemj.ThruDate = DateTime.Now;
                        itemj.PhysicalDataMixingMasterId = itemj.PhysicalDataMixingMasterId;
                        itemj.IsActive = false;
                        itemj.CreatedBy = itemj.CreatedBy;
                        itemj.ModifiedBy = username;
                        itemj.ModifiedDate = DateTime.Now;



                        _EahouseContext.PhysicalDataMixing.Update(itemj);
                        _EahouseContext.SaveChanges();
                    }
                }
            }

            catch (Exception)
            {


            }

        }
        public List<PhysicalDataMixingMasterRequestModel> GetDistinctPhysicalbyId(GetByPhysicalDataMixingMasterIdRequestModel IdDataMixing)
        {

            var data = (from s in _EahouseContext.PhysicalDataMixingMaster
                        join pddMap in _EahouseContext.DataDictionaryStatus
                         on s.IsApproved equals pddMap.ApprovalStatusId
                        join mix in _EahouseContext.DataMixingMaster
                       on s.DataMixingMasterId equals mix.DataMixingMasterId
                        where s.IsActive == true

                        select new PhysicalDataMixingMasterRequestModel
                        {
                            PhysicalDataMixingMasterId = s.PhysicalDataMixingMasterId,
                            ApprovalStatusName = pddMap.ApprovalStatusName,
                            PhysicalDataMixingVersion = s.PhysicalDataMixingVersion,
                            PhysicalDataMixingVersionDescription = s.PhysicalDataMixingVersionDescription,
                            FromDate = s.FromDate,
                            ExcelLink = s.ExcelLink,
                            ApprovalMailLink = s.ApprovalMailLink,
                            IsActive = s.IsActive,
                            DataMixingMasterId=s.DataMixingMasterId,
                            DataMixingVersion = mix.DataMixingVersion

                        }).Where(v => v.PhysicalDataMixingMasterId == IdDataMixing.Id).ToList();
            return data;
        }
        public void UpdatePhysicalDataMixingMaster([FromBody]PhysicalDataMixingMasterUpdate newData)
        {
            try
            {
                var role = hostingDictionary.HttpContext.Current.User.Claims.ToList();
                var username = role[0].Value;
                var oldData = _EahouseContext.PhysicalDataMixingMaster.FirstOrDefault(x => x.PhysicalDataMixingMasterId == newData.PhysicalDataMixingMasterId);
                oldData.PhysicalDataMixingMasterId = oldData.PhysicalDataMixingMasterId;
                oldData.PhysicalDataMixingVersion = oldData.PhysicalDataMixingVersion;
                oldData.PhysicalDataMixingVersionDescription = newData.PhysicalDataMixingVersionDescription;
                oldData.IsApproved = oldData.IsApproved;
                oldData.FromDate = oldData.FromDate;
                oldData.ExcelLink = newData.ExcelLink;
                oldData.ApprovalMailLink = newData.ApprovalMailLink;
                oldData.DataMixingMasterId = oldData.DataMixingMasterId;
                oldData.CreatedBy = oldData.CreatedBy;
                oldData.ModifiedBy = username;
                oldData.ModifiedDate = DateTime.Now;


                _EahouseContext.PhysicalDataMixingMaster.Update(oldData);
                _EahouseContext.SaveChanges();
            }
            catch (Exception)
            {


            }
        }
        public List<PhysicalDataMixingMailDocument> GetDocumentMailById(PhysicalDataMixingIdRequestModel IdDocument)
        {


            var data = (from s in _EahouseContext.PhysicalDataMixingMailDocument



                        select new PhysicalDataMixingMailDocument
                        {
                            PhysicalDataMixingMailDocumentId = s.PhysicalDataMixingMailDocumentId,
                            FileName = s.FileName,
                            ContentType = s.ContentType,
                            FileSize = s.FileSize,
                            PhysicalDataMixingMasterId = s.PhysicalDataMixingMasterId
                        }).Where(v => v.PhysicalDataMixingMasterId == IdDocument.PhysicalDataMixingMasterId).ToList();
            return data;
        }
        public List<PhysicalDataMixingExcelDocument> GetDocumentExcelById(PhysicalDataMixingIdRequestModel IdDocument)
        {


            var data = (from s in _EahouseContext.PhysicalDataMixingExcelDocument



                        select new PhysicalDataMixingExcelDocument
                        {
                            PhysicalDataMixingExcelDocumentId = s.PhysicalDataMixingExcelDocumentId,
                            FileName = s.FileName,
                            ContentType = s.ContentType,
                            FileSize = s.FileSize,
                            PhysicalDataMixingMasterId = s.PhysicalDataMixingMasterId
                        }).Where(v => v.PhysicalDataMixingMasterId == IdDocument.PhysicalDataMixingMasterId).ToList();
            return data;

        }
        public IResult AddPhysicalDataMixingMaster([FromBody]PhysicalDataMixingMaster newData)
        {
            
                var role = hostingDictionary.HttpContext.Current.User.Claims.ToList();
                var username = role[0].Value;
            var dmm = new PhysicalDataMixingMaster();
                var data = 0;
                var dataMaster = (from s in _EahouseContext.PhysicalDataMixingMaster



                                  select new PhysicalDataMixingMaster
                                  {
                                      PhysicalDataMixingMasterId = s.PhysicalDataMixingMasterId,
                                      PhysicalDataMixingVersion = s.PhysicalDataMixingVersion,
                                      PhysicalDataMixingVersionDescription = s.PhysicalDataMixingVersionDescription,
                                      IsApproved = s.IsApproved,
                                      FromDate = s.FromDate,
                                      ExcelLink = s.ExcelLink,
                                      ApprovalMailLink = s.ApprovalMailLink,
                                      DataMixingMasterId=s.DataMixingMasterId,
                                      IsActive = s.IsActive
                                  }).ToList();

                foreach (var i in dataMaster)
                {


                    var item = _EahouseContext.PhysicalDataMixingMaster.FirstOrDefault(o => o.PhysicalDataMixingMasterId == i.PhysicalDataMixingMasterId);
                    if (item == null)
                    {

                        data = 0;
                    }
                    else
                    {
                        data = (from MaxValue in _EahouseContext.PhysicalDataMixingMaster
                                select MaxValue.PhysicalDataMixingMasterId
                           ).Max();
                    }
                }
                dmm.PhysicalDataMixingMasterId = data + 1;
                dmm.PhysicalDataMixingVersion = newData.PhysicalDataMixingVersion;
                dmm.PhysicalDataMixingVersionDescription = newData.PhysicalDataMixingVersionDescription;
                dmm.IsApproved = newData.IsApproved;
                dmm.FromDate = DateTime.Now;
                dmm.ExcelLink = newData.ExcelLink;
                dmm.ApprovalMailLink = newData.ApprovalMailLink;
                dmm.DataMixingMasterId = newData.DataMixingMasterId;
                dmm.CreatedBy = username;
                dmm.ModifiedBy = null;
                dmm.ModifiedDate = null;

                dmm.IsActive = true;




                _EahouseContext.PhysicalDataMixingMaster.Add(dmm);
                _EahouseContext.SaveChanges();


                var dataMailDocument = _EahouseContext.PhysicalDataMixingMailDocument.FirstOrDefault(x => x.PhysicalDataMixingMailDocumentId == newData.ApprovalMailLink);
                if (dataMailDocument != null)
                {



                    dataMailDocument.PhysicalDataMixingMailDocumentId = dataMailDocument.PhysicalDataMixingMailDocumentId;
                    dataMailDocument.FileName = dataMailDocument.FileName;
                    dataMailDocument.ContentType = dataMailDocument.ContentType;
                    dataMailDocument.FileSize = dataMailDocument.FileSize;
                    dataMailDocument.PhysicalDataMixingMasterId = (data + 1);
                    _EahouseContext.PhysicalDataMixingMailDocument.Update(dataMailDocument);
                    _EahouseContext.SaveChanges();


                }

                var dataExcelDocument = _EahouseContext.PhysicalDataMixingExcelDocument.FirstOrDefault(x => x.PhysicalDataMixingExcelDocumentId == newData.ExcelLink);
                if (dataExcelDocument != null)
                {



                    dataExcelDocument.PhysicalDataMixingExcelDocumentId = dataExcelDocument.PhysicalDataMixingExcelDocumentId;
                    dataExcelDocument.FileName = dataExcelDocument.FileName;
                    dataExcelDocument.ContentType = dataExcelDocument.ContentType;
                    dataExcelDocument.FileSize = dataExcelDocument.FileSize;
                    dataExcelDocument.PhysicalDataMixingMasterId = (data + 1);
                    _EahouseContext.PhysicalDataMixingExcelDocument.Update(dataExcelDocument);
                    _EahouseContext.SaveChanges();


                }
                return new SuccessResult((data + 1).ToString());
           
        }
        public List<ApprovalStatusNameRequest> GetApproval()
        {

            var data = (from s in _EahouseContext.DataDictionaryStatus

                        where s.ApprovalStatusId == 5

                        select new ApprovalStatusNameRequest
                        {
                            ApprovalStatusId = s.ApprovalStatusId,
                            ApprovalStatusName = s.ApprovalStatusName
                        }).ToList();
            return data;
        }
        public List<DataMixingMasterRequestModel> GetDataMixingMaster()
        {

            var dataMixingMaster = (from s in _EahouseContext.DataMixingMaster
                                    join st in _EahouseContext.DataDictionaryStatus
                                  on s.IsApproved equals st.ApprovalStatusId

                                    where s.IsActive == true && s.IsApproved==1

                        select new DataMixingMasterRequestModel
                        {
                            DataMixingMasterId = s.DataMixingMasterId,
                            ApprovalStatusName = st.ApprovalStatusName,
                            DataMixingVersion = s.DataMixingVersion,
                            DataMixingVersionDescription = s.DataMixingVersionDescription,
                            FromDate = s.FromDate,
                            ExcelLink = s.ExcelLink,
                            ApprovalMailLink = s.ApprovalMailLink,
                            IsActive = s.IsActive
                        }).ToList();

            var data = 0;
            foreach (var i in dataMixingMaster)
            {


                var item = _EahouseContext.DataMixingMaster.FirstOrDefault(o => o.DataMixingMasterId == i.DataMixingMasterId);
                if (item == null)
                {

                    data = 0;
                }
                else
                {
                    data = (from MaxValue in dataMixingMaster
                            select MaxValue.DataMixingMasterId
                       ).Max();
                }
            }
            var datamixingMasterMax  = (from s in _EahouseContext.DataMixingMaster
                                                             join st in _EahouseContext.DataDictionaryStatus
                                                           on s.IsApproved equals st.ApprovalStatusId

                                                             where s.IsActive == true && s.IsApproved == 1 && s.DataMixingMasterId==data

                                                             select new DataMixingMasterRequestModel
                                                             {
                                                                 DataMixingMasterId = s.DataMixingMasterId,
                                                                 ApprovalStatusName = st.ApprovalStatusName,
                                                                 DataMixingVersion = s.DataMixingVersion,
                                                                 DataMixingVersionDescription = s.DataMixingVersionDescription,
                                                                 FromDate = s.FromDate,
                                                                 ExcelLink = s.ExcelLink,
                                                                 ApprovalMailLink = s.ApprovalMailLink,
                                                                 IsActive = s.IsActive
                                                             }).ToList();

            return datamixingMasterMax;
        }
        public List<PhysicalDataMixingRequestModel> GetDataMixingPhysical(PhysicalDataMixingMasterIdModel masterModel)
        {

            var data = (from s in _EahouseContext.PhysicalDataMixing
                        where s.IsActive==true
                        select new PhysicalDataMixingRequestModel
                        {
                            DataDictionaryItemId = s.DataDictionaryItemId,
                            DDItemServerName = s.DDItemServerName,
                            PhysicalDataMixingDescription = s.PhysicalDataMixingDescription,
                            DDItemDatabaseName = s.DDItemDatabaseName,
                            FromDate = s.FromDate,
                            DDItemSchemaName = s.DDItemSchemaName,
                            DDItemTableName = s.DDItemTableName,
                            DDItemColumnName = s.DDItemColumnName,
                            PhysicalDataMixingRuleName = s.PhysicalDataMixingRuleName,
                            PhysicalDataMixingId = s.PhysicalDataMixingId,
                            ThruDate = s.ThruDate,
                            PhysicalDataMixingMasterId = s.PhysicalDataMixingMasterId,
                            IsActive = s.IsActive,
                            CreatedBy = s.CreatedBy,
                            ModifiedBy = s.ModifiedBy,
                            ModifiedDate = s.ModifiedDate,

                        }).Where(v => v.PhysicalDataMixingMasterId == masterModel.PhysicalDataMixingMasterId).OrderBy(o => o.DataDictionaryItemId).ThenBy(x => x.FromDate).ToList();
     
           

            return data;
        }
        public void UpdatePhysicalDataMixingMasterStatus([FromBody]PhysicalDataMixingMasterUpdate newData)
        {
            try
            {
                var role = hostingDictionary.HttpContext.Current.User.Claims.ToList();
                var username = role[0].Value;
                var oldData = _EahouseContext.PhysicalDataMixingMaster.FirstOrDefault(x => x.PhysicalDataMixingMasterId == newData.PhysicalDataMixingMasterId);
                oldData.DataMixingMasterId = oldData.DataMixingMasterId;
                oldData.PhysicalDataMixingVersion = oldData.PhysicalDataMixingVersion;
                oldData.PhysicalDataMixingVersionDescription = oldData.PhysicalDataMixingVersionDescription;
                oldData.IsApproved = newData.IsApproved;
                oldData.FromDate = oldData.FromDate;
                oldData.ExcelLink = oldData.ExcelLink;
                oldData.ApprovalMailLink = oldData.ApprovalMailLink;
                oldData.ModifiedBy = username;
                oldData.ModifiedDate = DateTime.Now;
                oldData.IsActive = true;


                _EahouseContext.PhysicalDataMixingMaster.Update(oldData);
                _EahouseContext.SaveChanges();
            }
            catch (Exception)
            {


            }
        }
        public void DeletePhysicalDataMixing([FromBody]PhysicalDataMixingDictionaryIdRequestModel model)
        {
            try
            {
                var role = hostingDictionary.HttpContext.Current.User.Claims.ToList();
                var username = role[0].Value;
                var mainData = _EahouseContext.PhysicalDataMixing.FirstOrDefault(x => x.PhysicalDataMixingId == model.PhysicalDataMixingId);
                mainData.DataDictionaryItemId = mainData.DataDictionaryItemId; ;
               
                mainData.PhysicalDataMixingRuleName = mainData.PhysicalDataMixingRuleName;
                mainData.PhysicalDataMixingDescription = mainData.PhysicalDataMixingDescription;
                mainData.FromDate = mainData.FromDate;
                mainData.ThruDate = DateTime.Now;
                mainData.CreatedBy = mainData.CreatedBy;
                mainData.ModifiedBy = username;
                mainData.ModifiedDate = DateTime.Now;
                mainData.IsActive = false;
                _EahouseContext.PhysicalDataMixing.Update(mainData);
                _EahouseContext.SaveChanges();
            }
            catch (Exception)
            {


            }

        }

       


        public void UpdatePhysicalDataMixingDetail([FromBody]PhysicalDataMixingDetailUpdateModel newData)
        {
            try
            {
                var role = hostingDictionary.HttpContext.Current.User.Claims.ToList();
                var username = role[0].Value;
                var dmm = new PhysicalDataMixing();
                var dmm2 = new PhysicalDataMixing();
                var data = 0;
                var oldData = _EahouseContext.PhysicalDataMixing.FirstOrDefault(x => x.PhysicalDataMixingId == newData.PhysicalDataMixingId);
                if (newData.DDItemColumnName.Length == 0 || newData.DDItemColumnName == null)
                {
                    oldData.DataDictionaryItemId = oldData.DataDictionaryItemId;
                    oldData.DDItemServerName = newData.DDItemServerName;
                    oldData.PhysicalDataMixingDescription = newData.PhysicalDataMixingDescription;
                    oldData.DDItemDatabaseName = newData.DDItemDatabaseName;
                    oldData.DDItemSchemaName = newData.DDItemSchemaName;
                    oldData.DDItemTableName = newData.DDItemTableName;
                    oldData.DDItemColumnName = null;
                    oldData.PhysicalDataMixingRuleName = newData.PhysicalDataMixingRuleName;
                    oldData.PhysicalDataMixingId = oldData.PhysicalDataMixingId;
                    oldData.FromDate = oldData.FromDate;
                    oldData.ThruDate = oldData.ThruDate;
                    oldData.PhysicalDataMixingMasterId = oldData.PhysicalDataMixingMasterId;
                    oldData.IsActive = oldData.IsActive;
                    oldData.CreatedBy = oldData.CreatedBy;
                    oldData.ModifiedBy = username;
                    oldData.ModifiedDate = DateTime.Now;

                    _EahouseContext.PhysicalDataMixing.Update(oldData);
                    _EahouseContext.SaveChanges();
                }
                else if (newData.DDItemColumnName.Length == 1)
                {
                    oldData.DataDictionaryItemId = oldData.DataDictionaryItemId;
                    oldData.DDItemServerName = newData.DDItemServerName;
                    oldData.PhysicalDataMixingDescription = newData.PhysicalDataMixingDescription;
                    oldData.DDItemDatabaseName = newData.DDItemDatabaseName;
                    oldData.DDItemSchemaName = newData.DDItemSchemaName;
                    oldData.DDItemTableName = newData.DDItemTableName;
                    oldData.DDItemColumnName = newData.DDItemColumnName[0];
                    oldData.PhysicalDataMixingRuleName = newData.PhysicalDataMixingRuleName;
                    oldData.PhysicalDataMixingId = oldData.PhysicalDataMixingId;
                    oldData.FromDate = oldData.FromDate;
                    oldData.ThruDate = oldData.ThruDate;
                    oldData.PhysicalDataMixingMasterId = oldData.PhysicalDataMixingMasterId;
                    oldData.IsActive = oldData.IsActive;
                    oldData.CreatedBy = oldData.CreatedBy;
                    oldData.ModifiedBy = username;
                    oldData.ModifiedDate = DateTime.Now;

                    _EahouseContext.PhysicalDataMixing.Update(oldData);
                    _EahouseContext.SaveChanges();
                }
                else if (newData.DDItemColumnName.Length > 1)
                {
                    for (var j = 0; j < newData.DDItemColumnName.Length; j++)
                    {
                        if (j == 0)
                        {
                            oldData.DataDictionaryItemId = oldData.DataDictionaryItemId;
                            oldData.DDItemServerName = newData.DDItemServerName;
                            oldData.PhysicalDataMixingDescription = newData.PhysicalDataMixingDescription;
                            oldData.DDItemDatabaseName = newData.DDItemDatabaseName;
                            oldData.DDItemSchemaName = newData.DDItemSchemaName;
                            oldData.DDItemTableName = newData.DDItemTableName;
                            oldData.DDItemColumnName = newData.DDItemColumnName[j];
                            oldData.PhysicalDataMixingRuleName = newData.PhysicalDataMixingRuleName;
                            oldData.PhysicalDataMixingId = oldData.PhysicalDataMixingId;
                            oldData.FromDate = oldData.FromDate;
                            oldData.ThruDate = oldData.ThruDate;
                            oldData.PhysicalDataMixingMasterId = oldData.PhysicalDataMixingMasterId;
                            oldData.IsActive = oldData.IsActive;
                            oldData.CreatedBy = oldData.CreatedBy;
                            oldData.ModifiedBy = username;
                            oldData.ModifiedDate = DateTime.Now;

                            _EahouseContext.PhysicalDataMixing.Update(oldData);
                            _EahouseContext.SaveChanges();
                        }

                        else
                        {

                            var dataMixingDetail = (from s in _EahouseContext.PhysicalDataMixing



                                                    select new PhysicalDataMixing
                                                    {


                                                        DataDictionaryItemId = s.DataDictionaryItemId,
                                                        DDItemServerName = s.DDItemServerName,
                                                        PhysicalDataMixingDescription = s.PhysicalDataMixingDescription,
                                                        DDItemDatabaseName = s.DDItemDatabaseName,
                                                        DDItemSchemaName = s.DDItemSchemaName,
                                                        DDItemTableName = s.DDItemTableName,
                                                        DDItemColumnName = s.DDItemColumnName,
                                                        PhysicalDataMixingRuleName = s.PhysicalDataMixingRuleName,
                                                        PhysicalDataMixingId = s.PhysicalDataMixingId,
                                                        FromDate = s.FromDate,
                                                        ThruDate = s.ThruDate,
                                                        PhysicalDataMixingMasterId = s.PhysicalDataMixingMasterId,
                                                        IsActive = s.IsActive,
                                                        CreatedBy = s.CreatedBy,
                                                        ModifiedBy = s.ModifiedBy,
                                                        ModifiedDate = s.ModifiedDate
                                                    }).ToList();

                            foreach (var i in dataMixingDetail)
                            {


                                var item = _EahouseContext.PhysicalDataMixing.FirstOrDefault(o => o.PhysicalDataMixingId == i.PhysicalDataMixingId);
                                if (item == null)
                                {

                                    data = 0;
                                }
                                else
                                {
                                    data = (from MaxValue in _EahouseContext.PhysicalDataMixing
                                            select MaxValue.PhysicalDataMixingId
                                       ).Max();
                                }
                            }
                            var checking = _EahouseContext.PhysicalDataMixing.FirstOrDefault(o => o.PhysicalDataMixingId == (data + 1));

                            var maxData = GetMaxId();

                            dmm.DataDictionaryItemId = newData.DataDictionaryItemId;
                            dmm.DDItemServerName = newData.DDItemServerName;
                            dmm.PhysicalDataMixingDescription = newData.PhysicalDataMixingDescription;
                            dmm.DDItemDatabaseName = newData.DDItemDatabaseName;
                            dmm.DDItemSchemaName = newData.DDItemSchemaName;
                            dmm.DDItemTableName = newData.DDItemTableName;
                            dmm.DDItemColumnName = newData.DDItemColumnName[j];
                            dmm.PhysicalDataMixingRuleName = newData.PhysicalDataMixingRuleName;
                            dmm.PhysicalDataMixingId = data + 1;
                            dmm.FromDate = DateTime.Now;
                            dmm.ThruDate = null;
                            dmm.PhysicalDataMixingMasterId = oldData.PhysicalDataMixingMasterId;
                            dmm.CreatedBy = username;
                            dmm.ModifiedBy = null;
                            dmm.ModifiedDate = null;
                            dmm.IsActive = true;




                            _EahouseContext.PhysicalDataMixing.Add(dmm);
                            _EahouseContext.SaveChanges();
                        }
                    }
                }
            }
            catch (Exception exp)
            {

                exp.ToString();
            }

        }
        public int GetMaxId()
        {
            var data = 0;
            var dataMixingDetail = (from s in _EahouseContext.PhysicalDataMixing



                                    select new PhysicalDataMixing
                                    {


                                        DataDictionaryItemId = s.DataDictionaryItemId,
                                        DDItemServerName = s.DDItemServerName,
                                        PhysicalDataMixingDescription = s.PhysicalDataMixingDescription,
                                        DDItemDatabaseName = s.DDItemDatabaseName,
                                        DDItemSchemaName = s.DDItemSchemaName,
                                        DDItemTableName = s.DDItemTableName,
                                        DDItemColumnName = s.DDItemColumnName,
                                        PhysicalDataMixingRuleName = s.PhysicalDataMixingRuleName,
                                        PhysicalDataMixingId = s.PhysicalDataMixingId,
                                        FromDate = s.FromDate,
                                        ThruDate = s.ThruDate,
                                        PhysicalDataMixingMasterId = s.PhysicalDataMixingMasterId,
                                        IsActive = s.IsActive,
                                        CreatedBy = s.CreatedBy,
                                        ModifiedBy = s.ModifiedBy,
                                        ModifiedDate = s.ModifiedDate
                                    }).ToList();

            foreach (var i in dataMixingDetail)
            {


                var item = _EahouseContext.PhysicalDataMixing.FirstOrDefault(o => o.PhysicalDataMixingId == i.PhysicalDataMixingId);
                if (item == null)
                {

                    data = 0;
                }
                else
                {
                    data = (from MaxValue in _EahouseContext.PhysicalDataMixing
                            select MaxValue.PhysicalDataMixingId
                       ).Max();
                }
            }

            return (data+1);
        }

        public IResult AddPhysicalDataMixingDetail([FromBody]PhysicalDataMixingAdd newData)
        {

            try
            {
                var role = hostingDictionary.HttpContext.Current.User.Claims.ToList();
                var username = role[0].Value;
                var dmm = new PhysicalDataMixing();
                var dmm2 = new PhysicalDataMixing();
                var data = 0;
                if (newData.DDItemColumnName.Length == 0 || newData.DDItemColumnName == null)
                {
                    var dataMixingDetail = (from s in _EahouseContext.PhysicalDataMixing

                                            select new PhysicalDataMixing
                                            {
                                                DataDictionaryItemId = s.DataDictionaryItemId,
                                                DDItemServerName = s.DDItemServerName,
                                                PhysicalDataMixingDescription = s.PhysicalDataMixingDescription,
                                                DDItemDatabaseName = s.DDItemDatabaseName,
                                                DDItemSchemaName = s.DDItemSchemaName,
                                                DDItemTableName = s.DDItemTableName,
                                                DDItemColumnName = s.DDItemColumnName,
                                                PhysicalDataMixingRuleName = s.PhysicalDataMixingRuleName,
                                                PhysicalDataMixingId = s.PhysicalDataMixingId,
                                                FromDate = s.FromDate,
                                                ThruDate = s.ThruDate,
                                                PhysicalDataMixingMasterId = s.PhysicalDataMixingMasterId,
                                                IsActive = s.IsActive,
                                                CreatedBy = s.CreatedBy,
                                                ModifiedBy = s.ModifiedBy,
                                                ModifiedDate = s.ModifiedDate
                                            }).ToList();

                    foreach (var i in dataMixingDetail)
                    {


                        var item = _EahouseContext.PhysicalDataMixing.FirstOrDefault(o => o.PhysicalDataMixingId == i.PhysicalDataMixingId);
                        if (item == null)
                        {

                            data = 0;
                        }
                        else
                        {
                            data = (from MaxValue in _EahouseContext.PhysicalDataMixing
                                    select MaxValue.PhysicalDataMixingId
                               ).Max();
                        }
                    }
                    var checking = _EahouseContext.PhysicalDataMixing.FirstOrDefault(o => o.PhysicalDataMixingId == (data + 1));

                    var maxData = GetMaxId();

                    dmm.DataDictionaryItemId = newData.DataDictionaryItemId;
                    dmm.DDItemServerName = newData.DDItemServerName;
                    dmm.PhysicalDataMixingDescription = newData.PhysicalDataMixingDescription;
                    dmm.DDItemDatabaseName = newData.DDItemDatabaseName;
                    dmm.DDItemSchemaName = newData.DDItemSchemaName;
                    dmm.DDItemTableName = newData.DDItemTableName;
                    dmm.DDItemColumnName = null;
                    dmm.PhysicalDataMixingRuleName = newData.PhysicalDataMixingRuleName;
                    dmm.PhysicalDataMixingId = data + 1;
                    dmm.FromDate = DateTime.Now;
                    dmm.ThruDate = null;
                    dmm.PhysicalDataMixingMasterId = newData.PhysicalDataMixingMasterId;
                    dmm.CreatedBy = username;
                    dmm.ModifiedBy = null;
                    dmm.ModifiedDate = null;
                    dmm.IsActive = true;

                    _EahouseContext.PhysicalDataMixing.Add(dmm);
                    _EahouseContext.SaveChanges();

                }
                else
                {
                    for (var j = 0; j < newData.DDItemColumnName.Length; j++)
                    {
                        var dataMixingDetail = (from s in _EahouseContext.PhysicalDataMixing

                                                select new PhysicalDataMixing
                                                {
                                                    DataDictionaryItemId = s.DataDictionaryItemId,
                                                    DDItemServerName = s.DDItemServerName,
                                                    PhysicalDataMixingDescription = s.PhysicalDataMixingDescription,
                                                    DDItemDatabaseName = s.DDItemDatabaseName,
                                                    DDItemSchemaName = s.DDItemSchemaName,
                                                    DDItemTableName = s.DDItemTableName,
                                                    DDItemColumnName = s.DDItemColumnName,
                                                    PhysicalDataMixingRuleName = s.PhysicalDataMixingRuleName,
                                                    PhysicalDataMixingId = s.PhysicalDataMixingId,
                                                    FromDate = s.FromDate,
                                                    ThruDate = s.ThruDate,
                                                    PhysicalDataMixingMasterId = s.PhysicalDataMixingMasterId,
                                                    IsActive = s.IsActive,
                                                    CreatedBy = s.CreatedBy,
                                                    ModifiedBy = s.ModifiedBy,
                                                    ModifiedDate = s.ModifiedDate
                                                }).ToList();

                        foreach (var i in dataMixingDetail)
                        {


                            var item = _EahouseContext.PhysicalDataMixing.FirstOrDefault(o => o.PhysicalDataMixingId == i.PhysicalDataMixingId);
                            if (item == null)
                            {

                                data = 0;
                            }
                            else
                            {
                                data = (from MaxValue in _EahouseContext.PhysicalDataMixing
                                        select MaxValue.PhysicalDataMixingId
                                   ).Max();
                            }
                        }
                        var checking = _EahouseContext.PhysicalDataMixing.FirstOrDefault(o => o.PhysicalDataMixingId == (data + 1));

                        var maxData = GetMaxId();

                        dmm.DataDictionaryItemId = newData.DataDictionaryItemId;
                        dmm.DDItemServerName = newData.DDItemServerName;
                        dmm.PhysicalDataMixingDescription = newData.PhysicalDataMixingDescription;
                        dmm.DDItemDatabaseName = newData.DDItemDatabaseName;
                        dmm.DDItemSchemaName = newData.DDItemSchemaName;
                        dmm.DDItemTableName = newData.DDItemTableName;
                        dmm.DDItemColumnName = newData.DDItemColumnName[j];
                        dmm.PhysicalDataMixingRuleName = newData.PhysicalDataMixingRuleName;
                        dmm.PhysicalDataMixingId = data + 1;
                        dmm.FromDate = DateTime.Now;
                        dmm.ThruDate = null;
                        dmm.PhysicalDataMixingMasterId = newData.PhysicalDataMixingMasterId;
                        dmm.CreatedBy = username;
                        dmm.ModifiedBy = null;
                        dmm.ModifiedDate = null;
                        dmm.IsActive = true;

                        _EahouseContext.PhysicalDataMixing.Add(dmm);
                        _EahouseContext.SaveChanges();
                    }

                }

                return new SuccessResult((data + 1).ToString());
            }
            catch (Exception exp)
            {

                return new ErrorResult(exp.ToString());
            }

        }
        public List<PhysicalDataMixinColumnResponse> GetColumnList1(PhysicalDataMixingIdRequest data)
        {
            
            var result = (from k in _EahouseContext.PhysicalDataMixing
                          
                          where k.PhysicalDataMixingId == data.PhysicalDataMixingId && k.IsActive == true
                          select new PhysicalDataMixinColumnResponse()
                          {
                              DDItemColumnName = k.DDItemColumnName,


                          }).ToList();
            
            return result.ToList();
        }
        public IResult UpdateDataWithDataMixing([FromBody]PhysicalDataMixing newData)

        {

            var role = hostingDictionary.HttpContext.Current.User.Claims.ToList();
            var username = role[0].Value;
            var dmm = new PhysicalDataMixing();
            var dmm2 = new PhysicalDataMixing();
            var data = 0;
            var dataMixingDetail = (from s in _EahouseContext.PhysicalDataMixing



                                    select new PhysicalDataMixing
                                    {


                                        DataDictionaryItemId = s.DataDictionaryItemId,
                                        DDItemServerName = s.DDItemServerName,
                                        PhysicalDataMixingDescription = s.PhysicalDataMixingDescription,
                                        DDItemDatabaseName = s.DDItemDatabaseName,
                                        DDItemSchemaName = s.DDItemSchemaName,
                                        DDItemTableName = s.DDItemTableName,
                                        DDItemColumnName = s.DDItemColumnName,
                                        PhysicalDataMixingRuleName = s.PhysicalDataMixingRuleName,
                                        PhysicalDataMixingId = s.PhysicalDataMixingId,
                                        FromDate = s.FromDate,
                                        ThruDate = s.ThruDate,
                                        PhysicalDataMixingMasterId = s.PhysicalDataMixingMasterId,
                                        IsActive = s.IsActive,
                                        CreatedBy = s.CreatedBy,
                                        ModifiedBy = s.ModifiedBy,
                                        ModifiedDate = s.ModifiedDate,
                                    }).AsNoTracking().ToList();
            var itemMixingMaster = _EahouseContext.PhysicalDataMixingMaster.AsNoTracking().FirstOrDefault(o => o.PhysicalDataMixingMasterId == newData.PhysicalDataMixingMasterId);

            var dataMixingMainDetail = (from s in _EahouseContext.DataMixing

                                        where s.DataMixingMasterId==1 && s.DataMixingRuleId==4


                                    select new DataMixing
                                    {
                                        DataDictionaryId = s.DataDictionaryId,
                                        DataDescription = s.DataDescription,
                                        DataMixingRuleId = s.DataMixingRuleId,
                                        DataMixingDescription = s.DataMixingDescription,
                                        FromDate = s.FromDate,
                                        ThruDate = s.ThruDate,
                                        DataMixingMasterId = s.DataMixingMasterId,
                                        DataMixingId = s.DataMixingId,
                                        DataMixingScopeId = s.DataMixingScopeId,
                                        IsActive = s.IsActive,

                                    }).Where(o=>o.DataMixingMasterId==1 && (o.DataMixingRuleId == 4 || o.DataMixingRuleId == 5)).AsNoTracking().ToList();
            var dataMixingMasterMain = (from s in _EahouseContext.DataMixing

                                        where s.DataMixingMasterId == 1 && s.DataMixingRuleId == 4


                                        select new DataMixing
                                        {
                                            DataDictionaryId = s.DataDictionaryId,
                                            DataDescription = s.DataDescription,
                                            DataMixingRuleId = s.DataMixingRuleId,
                                            DataMixingDescription = s.DataMixingDescription,
                                            FromDate = s.FromDate,
                                            ThruDate = s.ThruDate,
                                            DataMixingMasterId = s.DataMixingMasterId,
                                            DataMixingId = s.DataMixingId,
                                            DataMixingScopeId = s.DataMixingScopeId,
                                            IsActive = s.IsActive,

                                        }).Where(o => o.DataMixingMasterId == itemMixingMaster.DataMixingMasterId).AsNoTracking().ToList();
            var dataCont=false;
            foreach (var ij in dataMixingDetail)
            {


                var item = _EahouseContext.PhysicalDataMixing.AsNoTracking().FirstOrDefault(o => o.PhysicalDataMixingId == ij.PhysicalDataMixingId);
                if (item == null)
                {

                    dataCont = true;
                }
               
            }

            foreach (var i in dataMixingMainDetail)
            {
                var itemMixing = _EahouseContext.PhysicalDataMixing.AsNoTracking().FirstOrDefault(o => o.DataDictionaryItemId == i.DataDictionaryId && o.PhysicalDataMixingMasterId==newData.PhysicalDataMixingMasterId);
               
                if (itemMixing == null)
                {
                   
                   

                          if (dataCont == true)
                        {

                            data = 0;
                        }
                        else
                        {
                            data = (from MaxValue in _EahouseContext.PhysicalDataMixing.AsNoTracking()
                                    select MaxValue.PhysicalDataMixingId
                               ).Max();
                        }
                    

                    dmm.DataDictionaryItemId = i.DataDictionaryId;
                    dmm.DDItemServerName = null; ;
                    dmm.PhysicalDataMixingDescription = null;
                    dmm.DDItemDatabaseName = null;
                    dmm.DDItemSchemaName = null;
                    dmm.DDItemTableName = null;
                    dmm.DDItemColumnName = null;
                    dmm.PhysicalDataMixingRuleName = null;
                    dmm.PhysicalDataMixingId = (data + 1);
                    dmm.FromDate = DateTime.Now;
                    dmm.ThruDate = null;
                    dmm.PhysicalDataMixingMasterId = newData.PhysicalDataMixingMasterId;
                    dmm.CreatedBy = username;
                    dmm.ModifiedBy = null;
                    dmm.ModifiedDate = null;
                    dmm.IsActive = true;
                    _EahouseContext.PhysicalDataMixing.Add(dmm);
                    _EahouseContext.SaveChanges();
                }
                else
                {
                    itemMixing.DataDictionaryItemId = i.DataDictionaryId;
                    itemMixing.DDItemServerName = null; ;
                    itemMixing.PhysicalDataMixingDescription = null;
                    itemMixing.DDItemDatabaseName = null;
                    itemMixing.DDItemSchemaName = null;
                    itemMixing.DDItemTableName = null;
                    itemMixing.DDItemColumnName =null;
                    itemMixing.PhysicalDataMixingRuleName =null;
                    itemMixing.PhysicalDataMixingId = itemMixing.PhysicalDataMixingId;
                    itemMixing.FromDate = i.FromDate;
                    itemMixing.ThruDate = DateTime.Now;
                    itemMixing.PhysicalDataMixingMasterId = newData.PhysicalDataMixingMasterId;
                    itemMixing.CreatedBy = itemMixing.CreatedBy;
                    itemMixing.ModifiedBy = username;
                    itemMixing.ModifiedDate = DateTime.Now;
                    itemMixing.IsActive = true;


                    _EahouseContext.PhysicalDataMixing.Update(itemMixing);
                    _EahouseContext.SaveChanges();
                }
            }



            return new SuccessResult((data + 0).ToString());

        }
        public List<PhysicalDataMixingApprovalStatus> GetApprovalStatusbyId(PhysicalDataMixingApprovalStatus IdDataMixing)
        {

            var data = (from s in _EahouseContext.PhysicalDataMixingMaster



                        select new PhysicalDataMixingApprovalStatus
                        {
                            PhysicalDataMixingMasterId = s.PhysicalDataMixingMasterId,
                            IsApproved = s.IsApproved,

                        }).Where(v => v.PhysicalDataMixingMasterId == IdDataMixing.PhysicalDataMixingMasterId).ToList();
            return data;
        }
        public int CheckNull(PhysicalDataMixing master)
        {
            var val = 0;

            var data = (from s in _EahouseContext.PhysicalDataMixing
                        where s.PhysicalDataMixingMasterId == master.PhysicalDataMixingMasterId && s.IsActive == true
                        select new PhysicalDataMixing
                        {
                            DataDictionaryItemId = s.DataDictionaryItemId,
                            DDItemServerName = s.DDItemServerName,
                            PhysicalDataMixingDescription = s.PhysicalDataMixingDescription,
                            DDItemDatabaseName = s.DDItemDatabaseName,
                            FromDate = s.FromDate,
                            DDItemSchemaName = s.DDItemSchemaName,
                            DDItemTableName = s.DDItemTableName,
                            DDItemColumnName = s.DDItemColumnName,
                            PhysicalDataMixingRuleName = s.PhysicalDataMixingRuleName,
                            PhysicalDataMixingId = s.PhysicalDataMixingId,
                            ThruDate = s.ThruDate,
                            PhysicalDataMixingMasterId = s.PhysicalDataMixingMasterId,
                            IsActive = s.IsActive,
                            CreatedBy = s.CreatedBy,
                            ModifiedBy = s.ModifiedBy,
                            ModifiedDate = s.ModifiedDate

                        }).ToList();


            foreach (var dx in data)
            {
                var itemMixing = _EahouseContext.PhysicalDataMixing.AsNoTracking().FirstOrDefault(o => o.PhysicalDataMixingId == dx.PhysicalDataMixingId);
                if (itemMixing.PhysicalDataMixingRuleName == null || itemMixing.PhysicalDataMixingRuleName == "" || itemMixing.PhysicalDataMixingRuleName == " ")
                {
                    return val = 10;
                }
                else
                {
                    val = 1;
                }
            }
            return val;
        }
        public int GetLastSnapshotVersion()
        {
            var data = (from MaxValue in _EahouseContext.SnapshotVersion
                        select MaxValue.SnapshotVersionId
                        ).Max();
            return data;
        }




        public List<string> GetServerList(ServerListModel request)
        {
            if (request.SnapshotVersion == null)
            {
                var data = _EahouseContext.SnapShotTable.Where(c => c.SnapshotVersionId == GetLastSnapshotVersion())
                                                        .Select(p => p.ServerName).Distinct().ToList();
                return data;
            }

            //var snapVersion = FindSnapshotVersionWithDate(request.StartDate,request.FinishDate);

            var dataWithDate = _EahouseContext.SnapShotTable.Where(c => c.SnapshotVersionId == request.SnapshotVersion)
                                                        .Select(p => p.ServerName).Distinct().ToList();
            return dataWithDate;
        }
        public List<string> GetDatabaseList(DatabaseListRequest request)
        {
            //request.ServerName = JsonConvert.DeserializeObject<string>(request.ServerName);
            //request.SnapshotVersion = JsonConvert.DeserializeObject<int>(request.SnapshotVersion);
            if (request.SnapshotVersion == null)
            {
                var data = _EahouseContext.SnapShotTable.Where(c => (c.SnapshotVersionId == GetLastSnapshotVersion()) &&
                                                                    (c.ServerName == request.ServerName))
                                                            .Select(p => p.DatabaseName).Distinct().ToList();
                return data;
            }
            //var snapVersion = FindSnapshotVersionWithDate(request.StartDate, request.FinishDate);

            var dataWithSnapshot = _EahouseContext.SnapShotTable.Where(c => (c.SnapshotVersionId == request.SnapshotVersion) &&
                                                                        (c.ServerName == request.ServerName))
                                                            .Select(p => p.DatabaseName).Distinct().ToList();
            //var ls =new  List<string>();
            //ls.Add(request.ServerName);
            return dataWithSnapshot;
        }
        public List<string> GetShemaList(GetSchemaModel getSchemaModel)
        {
            var data = _EahouseContext.PhysicalDataDictionary.Where(p => p.DatabaseName == getSchemaModel.DatabaseName && p.ServerName == getSchemaModel.ServerName && p.SchemaName != null).Select(p => p.SchemaName).Distinct().ToList();
            return data;
             
        }

        public List<string> GetTableList(GetTableModel getTableModel)
        {
            var data = _EahouseContext.PhysicalDataDictionary.Where(p => p.DatabaseName == getTableModel.DatabaseName && p.ServerName == getTableModel.ServerName && p.SchemaName == getTableModel.SchemaName && p.TableName != null).Select(p => p.TableName).Distinct().ToList();
            return data;
            
        }
        public List<string> GetColumnList(GetColumnModel getColumnModel)
        { var newList = new List<string>() { "Hepsi"};
            var data = _EahouseContext.PhysicalDataDictionary.Where(p => p.DatabaseName == getColumnModel.DatabaseName && p.ServerName == getColumnModel.ServerName && p.SchemaName == getColumnModel.SchemaName && p.TableName== getColumnModel.TableName && p.ColumnName != null).Select(p => p.ColumnName).Distinct().ToList();
           
            var allData= data.Concat(newList).ToList();
            return allData;

        }
        public List<string> GetRuleList()
        {
            
                var data = _EahouseContext.PhysicalDataMixingRule.Where(a=>a.IsActive==true).Select(p => p.PhysicalDataMixingRuleName).ToList();
            
                return data;
           
        }
        public List<KeyValueResponseModel> GetDataMixingRuleList()
        {

            var result = (from k in _EahouseContext.PhysicalDataMixingRule
                          orderby k.PhysicalDataMixingRuleId
                          select new KeyValueResponseModel
                          {
                              Key = k.PhysicalDataMixingRuleId,
                              Value = k.PhysicalDataMixingRuleName
                          });

            return result.ToList();
        }
        public List<KeyValueResponseModel> GetByPhysicalDataMixingVerison()
        {
            var result = (from PhysicalDataMixingMaster in _EahouseContext.PhysicalDataMixingMaster
                          where PhysicalDataMixingMaster.IsActive == true
                          select new KeyValueResponseModel()
                          {
                              Key = PhysicalDataMixingMaster.PhysicalDataMixingMasterId,
                              Value = PhysicalDataMixingMaster.PhysicalDataMixingVersion
                          }).ToList();

            return result;
        }
        public List<KeyValueResponseModel> GetByPhysicalDataMixingRule()
        {
            var result = (from PhysicalDataMixingRule in _EahouseContext.PhysicalDataMixingRule
                          where PhysicalDataMixingRule.IsActive == true
                          select new KeyValueResponseModel()
                          {
                              Key = PhysicalDataMixingRule.PhysicalDataMixingRuleId,
                              Value = PhysicalDataMixingRule.PhysicalDataMixingRuleName
                          }).ToList();

            return result;
        }
        public List<KeyValueResponseModel> GetByPhysicalDataMixingTable(GetByPhysicalDataMixingMasterIdRequestModel IdDataMixing)
        {
            
            var result0 = (from pdm in _EahouseContext.PhysicalDataMixing
                           where pdm.PhysicalDataMixingMasterId== IdDataMixing.Id &&pdm.IsActive==true
                           select new KeyValueResponseModel()
                          {
                              Key = pdm.PhysicalDataMixingId,
                              Value = pdm.DDItemTableName,
                          }).ToList();

            var result = result0.GroupBy(u => u.Value).Select((b, index) => new KeyValueResponseModel
            {
                
                Key = index+1,
                Value = b.First().Value,
              
            }).ToList();

            return result;
        }



       
        public IResult UpdateDataWithDataMixingy([FromBody]PhysicalDataMixing newData)
        {
            var dataMasterMax = 0;
            var role = hostingDictionary.HttpContext.Current.User.Claims.ToList();
            var username = role[0].Value;

            var dataMaster = (from s in _EahouseContext.DataMixingMaster

                              where s.IsActive == true && s.IsApproved == 1

                              select new DataMixingMaster
                              {
                                  DataMixingMasterId = s.DataMixingMasterId,
                                  DataMixingVersion = s.DataMixingVersion,
                                  DataMixingVersionDescription = s.DataMixingVersionDescription,
                                  IsApproved = s.IsApproved,
                                  FromDate = s.FromDate,
                                  ExcelLink = s.ExcelLink,
                                  ApprovalMailLink = s.ApprovalMailLink,
                                  IsActive = s.IsActive
                              }).AsNoTracking().ToList();


            foreach (var i in dataMaster)
            {


                var item = _EahouseContext.DataMixingMaster.FirstOrDefault(o => o.DataMixingMasterId == i.DataMixingMasterId );
                if (item == null)
                {

                    dataMasterMax = 0;
                }
                else
                {
                    dataMasterMax = (from MaxValue in dataMaster
                                     select MaxValue.DataMixingMasterId
                       ).Max();
                }

            }

            var dataMixingDetailMain = (from s in _EahouseContext.DataMixing
                                        where s.DataMixingMasterId == dataMasterMax && (s.DataMixingRuleId == 4|| s.DataMixingRuleId==5) && s.IsActive==true
                                        select new DataMixing
                                        {
                                            DataDictionaryId = s.DataDictionaryId,
                                            DataDescription = s.DataDescription,
                                            DataMixingRuleId = s.DataMixingRuleId,
                                            DataMixingDescription = s.DataMixingDescription,
                                            FromDate = s.FromDate,
                                            ThruDate = s.ThruDate,
                                            DataMixingMasterId = s.DataMixingMasterId,
                                            DataMixingId = s.DataMixingId,
                                            DataMixingScopeId = s.DataMixingScopeId,
                                            IsActive = s.IsActive,

                                        }).AsNoTracking().ToList();//.AsNoTracking was added because of tracking error
            var dataMixingDetail = (from s in _EahouseContext.PhysicalDataMixing
                                   


                                    select new PhysicalDataMixing
                                    {


                                        DataDictionaryItemId = s.DataDictionaryItemId,
                                        DDItemServerName = s.DDItemServerName,
                                        PhysicalDataMixingDescription = s.PhysicalDataMixingDescription,
                                        DDItemDatabaseName = s.DDItemDatabaseName,
                                        DDItemSchemaName = s.DDItemSchemaName,
                                        DDItemTableName = s.DDItemTableName,
                                        DDItemColumnName = s.DDItemColumnName,
                                        PhysicalDataMixingRuleName = s.PhysicalDataMixingRuleName,
                                        PhysicalDataMixingId = s.PhysicalDataMixingId,
                                        FromDate = s.FromDate,
                                        ThruDate = s.ThruDate,
                                        PhysicalDataMixingMasterId = s.PhysicalDataMixingMasterId,
                                        IsActive = s.IsActive,
                                        CreatedBy = s.CreatedBy,
                                        ModifiedBy = s.ModifiedBy,
                                        ModifiedDate = s.ModifiedDate,
                                    }).AsNoTracking().ToList();
           
            var dataMixingDetail5 = (from s in _EahouseContext.PhysicalDataMixing

                                    

                                     select new PhysicalDataMixing
                                     {


                                         DataDictionaryItemId = s.DataDictionaryItemId,
                                         DDItemServerName = s.DDItemServerName,
                                         PhysicalDataMixingDescription = s.PhysicalDataMixingDescription,
                                         DDItemDatabaseName = s.DDItemDatabaseName,
                                         DDItemSchemaName = s.DDItemSchemaName,
                                         DDItemTableName = s.DDItemTableName,
                                         DDItemColumnName = s.DDItemColumnName,
                                         PhysicalDataMixingRuleName = s.PhysicalDataMixingRuleName,
                                         PhysicalDataMixingId = s.PhysicalDataMixingId,
                                         FromDate = s.FromDate,
                                         ThruDate = s.ThruDate,
                                         PhysicalDataMixingMasterId = s.PhysicalDataMixingMasterId,
                                         IsActive = s.IsActive,
                                         CreatedBy = s.CreatedBy,
                                         ModifiedBy = s.ModifiedBy,
                                         ModifiedDate = s.ModifiedDate,
                                     }).AsNoTracking().ToList();
            var dataCont = false;
            foreach (var ij in dataMixingDetail)
            {


                var item = _EahouseContext.PhysicalDataMixing.AsNoTracking().FirstOrDefault(o => o.PhysicalDataMixingId == ij.PhysicalDataMixingId);
                if (item == null)
                {

                    dataCont = true;
                }

            }

            var physicalDataMaster = (from s in _EahouseContext.PhysicalDataMixingMaster



                                      select new PhysicalDataMixingMaster
                                      {
                                          PhysicalDataMixingMasterId = s.PhysicalDataMixingMasterId,
                                          PhysicalDataMixingVersion = s.PhysicalDataMixingVersion,
                                          PhysicalDataMixingVersionDescription = s.PhysicalDataMixingVersionDescription,
                                          IsApproved = s.IsApproved,
                                          FromDate = s.FromDate,
                                          ExcelLink = s.ExcelLink,
                                          ApprovalMailLink = s.ApprovalMailLink,
                                          DataMixingMasterId = s.DataMixingMasterId,
                                          IsActive = s.IsActive
                                      }).AsNoTracking().ToList();
            var count = 0;
            foreach (var i in dataMixingDetail5)
            {
                var item_master = _EahouseContext.PhysicalDataMixing.FirstOrDefault(o => o.PhysicalDataMixingMasterId == i.PhysicalDataMixingMasterId && o.IsActive == true);
                var item_master2 = _EahouseContext.PhysicalDataMixingMaster.FirstOrDefault(o => o.PhysicalDataMixingMasterId == i.PhysicalDataMixingMasterId && o.IsActive == true && newData.PhysicalDataMixingMasterId <= i.PhysicalDataMixingMasterId);
                if (item_master != null)
                {
                    if (item_master2 == null)
                    {
                        count = i.PhysicalDataMixingMasterId;
                    }



                }




            }
            var dataMixingDetail2 = (from s in _EahouseContext.PhysicalDataMixing

                                     where s.PhysicalDataMixingMasterId == count && s.IsActive==true

                                     select new PhysicalDataMixing
                                     {


                                         DataDictionaryItemId = s.DataDictionaryItemId,
                                         DDItemServerName = s.DDItemServerName,
                                         PhysicalDataMixingDescription = s.PhysicalDataMixingDescription,
                                         DDItemDatabaseName = s.DDItemDatabaseName,
                                         DDItemSchemaName = s.DDItemSchemaName,
                                         DDItemTableName = s.DDItemTableName,
                                         DDItemColumnName = s.DDItemColumnName,
                                         PhysicalDataMixingRuleName = s.PhysicalDataMixingRuleName,
                                         PhysicalDataMixingId = s.PhysicalDataMixingId,
                                         FromDate = s.FromDate,
                                         ThruDate = s.ThruDate,
                                         PhysicalDataMixingMasterId = s.PhysicalDataMixingMasterId,
                                         IsActive = s.IsActive,
                                         CreatedBy = s.CreatedBy,
                                         ModifiedBy = s.ModifiedBy,
                                         ModifiedDate = s.ModifiedDate
                                     }).AsNoTracking().ToList();
            var dataMixingDetail_new = (from s in _EahouseContext.PhysicalDataMixing

                                     where s.PhysicalDataMixingMasterId == newData.PhysicalDataMixingMasterId 

                                     select new PhysicalDataMixing
                                     {


                                         DataDictionaryItemId = s.DataDictionaryItemId,
                                         DDItemServerName = s.DDItemServerName,
                                         PhysicalDataMixingDescription = s.PhysicalDataMixingDescription,
                                         DDItemDatabaseName = s.DDItemDatabaseName,
                                         DDItemSchemaName = s.DDItemSchemaName,
                                         DDItemTableName = s.DDItemTableName,
                                         DDItemColumnName = s.DDItemColumnName,
                                         PhysicalDataMixingRuleName = s.PhysicalDataMixingRuleName,
                                         PhysicalDataMixingId = s.PhysicalDataMixingId,
                                         FromDate = s.FromDate,
                                         ThruDate = s.ThruDate,
                                         PhysicalDataMixingMasterId = s.PhysicalDataMixingMasterId,
                                         IsActive = s.IsActive,
                                         CreatedBy = s.CreatedBy,
                                         ModifiedBy = s.ModifiedBy,
                                         ModifiedDate = s.ModifiedDate
                                     }).AsNoTracking().ToList();
            var dataMixingIdMax = 0;

            var dmm = new PhysicalDataMixing();
            var dmmNewDataMixing = new PhysicalDataMixing();

            foreach (var y in dataMixingDetail_new)
            {
                var itemExistingChecking = _EahouseContext.PhysicalDataMixing.FirstOrDefault(a => a.PhysicalDataMixingMasterId == y.PhysicalDataMixingMasterId && a.PhysicalDataMixingMasterId==newData.PhysicalDataMixingMasterId );
                if (itemExistingChecking != null)
                {
                    _EahouseContext.RemoveRange(_EahouseContext.PhysicalDataMixing.Where(x => x.PhysicalDataMixingId == y.PhysicalDataMixingId).ToList());
                    _EahouseContext.SaveChanges();
                }
            }
               
            if (count != 0)
            {
                foreach (var o in dataMixingDetail2)
            {
                var itemMixing = _EahouseContext.PhysicalDataMixing.FirstOrDefault(a => a.PhysicalDataMixingMasterId==newData.PhysicalDataMixingMasterId && a.PhysicalDataMixingId==o.PhysicalDataMixingId && o.IsActive==true);
                   

                    if (itemMixing == null)
                    {
                        if (dataCont == true)
                        {

                            dataMixingIdMax = 0;
                        }
                        else
                        {
                            dataMixingIdMax = (from MaxValue in _EahouseContext.PhysicalDataMixing.AsNoTracking()
                                               select MaxValue.PhysicalDataMixingId
                               ).Max();
                        }
                        dmm.DataDictionaryItemId = o.DataDictionaryItemId;
                        dmm.DDItemServerName = o.DDItemServerName;
                        dmm.PhysicalDataMixingDescription = o.PhysicalDataMixingDescription;
                        dmm.DDItemDatabaseName = o.DDItemDatabaseName;
                        dmm.DDItemSchemaName = o.DDItemSchemaName;
                        dmm.DDItemTableName = o.DDItemTableName;
                        dmm.DDItemColumnName = o.DDItemColumnName;
                        dmm.PhysicalDataMixingRuleName = o.PhysicalDataMixingRuleName;
                        dmm.PhysicalDataMixingId = (dataMixingIdMax + 1);
                        dmm.FromDate = o.FromDate;
                        dmm.ThruDate = DateTime.Now;
                        dmm.PhysicalDataMixingMasterId = newData.PhysicalDataMixingMasterId;
                        dmm.CreatedBy = username;
                        dmm.ModifiedBy = username;
                        dmm.ModifiedDate = DateTime.Now;
                        dmm.IsActive = o.IsActive;
                        _EahouseContext.PhysicalDataMixing.Add(dmm);
                        _EahouseContext.SaveChanges();
                    }
                   
                  
                }
            }
           
                foreach (var l in dataMixingDetailMain)
                {
                    var itemDataMixing = _EahouseContext.PhysicalDataMixing.AsNoTracking().FirstOrDefault(o => o.DataDictionaryItemId == l.DataDictionaryId && o.PhysicalDataMixingMasterId == newData.PhysicalDataMixingMasterId && o.IsActive == true);
                    if (itemDataMixing == null)
                    {
                        if (dataCont == true)
                        {

                            dataMixingIdMax = 0;
                        }
                        else
                        {
                            dataMixingIdMax = (from MaxValue in _EahouseContext.PhysicalDataMixing.AsNoTracking()
                                               select MaxValue.PhysicalDataMixingId
                               ).Max();
                        }
                        dmmNewDataMixing.DataDictionaryItemId = l.DataDictionaryId;
                        dmmNewDataMixing.DDItemServerName = null;
                        dmmNewDataMixing.PhysicalDataMixingDescription = null;
                        dmmNewDataMixing.DDItemDatabaseName = null;
                        dmmNewDataMixing.DDItemSchemaName = null;
                        dmmNewDataMixing.DDItemTableName = null;
                        dmmNewDataMixing.DDItemColumnName = null;
                        dmmNewDataMixing.PhysicalDataMixingRuleName = null;
                        dmmNewDataMixing.PhysicalDataMixingId = (dataMixingIdMax + 1);
                        dmmNewDataMixing.FromDate = DateTime.Now;
                        dmmNewDataMixing.ThruDate = null;
                        dmmNewDataMixing.PhysicalDataMixingMasterId = newData.PhysicalDataMixingMasterId;
                        dmmNewDataMixing.CreatedBy = username;
                        dmmNewDataMixing.ModifiedBy = null;
                        dmmNewDataMixing.ModifiedDate = null;
                        dmmNewDataMixing.IsActive = true;
                        _EahouseContext.PhysicalDataMixing.Add(dmmNewDataMixing);
                        _EahouseContext.SaveChanges();

                    }
                 



                }

                
          

            return new SuccessResult(dataMasterMax.ToString());
        }
        public List<KeyValueResponseModel> GetPhysicalDataMixingRuleList()
        {

            var result = (from k in _EahouseContext.PhysicalDataMixingRule
                          orderby k.PhysicalDataMixingRuleId
                          where k.IsActive == true

                          select new KeyValueResponseModel
                          {
                              Key = k.PhysicalDataMixingRuleId,
                              Value = k.PhysicalDataMixingRuleName
                          });

            return result.ToList();
        }
        public IResult AddPhysicalDataMixingRule([FromBody]AddParameterModel newData)
        {
            try
            {
                var search = _EahouseContext.PhysicalDataMixingRule.FirstOrDefault(c => c.PhysicalDataMixingRuleName == newData.Name);
                if (search != null)
                {
                    search.IsActive = true;
                    _EahouseContext.Update(search);
                    _EahouseContext.SaveChanges();
                    return new SuccessResult("Ekleme işlemi başarılı.");
                }
                else
                {
                    var dmm = new PhysicalDataMixingRule();
                    var data = 0;
                    var dataRule = (from s in _EahouseContext.PhysicalDataMixingRule



                                    select new PhysicalDataMixingRule
                                    {
                                        PhysicalDataMixingRuleId = s.PhysicalDataMixingRuleId,
                                        PhysicalDataMixingRuleName = s.PhysicalDataMixingRuleName,
                                        IsActive = s.IsActive

                                    }).ToList();

                    foreach (var i in dataRule)
                    {


                        var item = _EahouseContext.PhysicalDataMixingRule.FirstOrDefault(o => o.PhysicalDataMixingRuleId == i.PhysicalDataMixingRuleId);
                        if (item == null)
                        {

                            data = 0;
                        }
                        else
                        {
                            data = (from MaxValue in _EahouseContext.PhysicalDataMixingRule
                                    select MaxValue.PhysicalDataMixingRuleId
                               ).Max();
                        }
                    }
                    dmm.PhysicalDataMixingRuleId = data + 1;
                    dmm.PhysicalDataMixingRuleName = newData.Name;
                    dmm.IsActive = true;





                    _EahouseContext.PhysicalDataMixingRule.Add(dmm);
                    _EahouseContext.SaveChanges();
                    return new SuccessResult("Ekleme işlemi başarılı.");
                }
            }


            catch (Exception exp)
            {

                return new ErrorResult(exp.ToString());
            }
        }
        public void DeleteRule([FromBody]DeleteParameterRequestModel model)
        {
            var data = _EahouseContext.PhysicalDataMixingRule.FirstOrDefault(x => x.PhysicalDataMixingRuleId == model.Id);
            data.IsActive = false;
            _EahouseContext.PhysicalDataMixingRule.Update(data);
            _EahouseContext.SaveChanges();
        }
        public void UpdateRule([FromBody]KeyValueResponseModel model)
        {
            var data = _EahouseContext.PhysicalDataMixingRule.FirstOrDefault(x => x.PhysicalDataMixingRuleId == model.Key);
            data.PhysicalDataMixingRuleName = model.Value;
            data.IsActive = true;
            _EahouseContext.PhysicalDataMixingRule.Update(data);
            _EahouseContext.SaveChanges();
        }
        public object GetByPhysicalDataMixingRuleById(PhysicalDataMixingRuleRequestModel request, DataSourceLoadOptionsBase loadOptions)
        {////

            if (request.PhysicalDataMixingRuleId ==0 || request == null || request.PhysicalDataMixingRuleId == null)
            {
                var allData = (from s in _EahouseContext.PhysicalDataMixing
                            where s.IsActive == true
                            select new PhysicalDataMixingRequestModel
                            {
                                DataDictionaryItemId = s.DataDictionaryItemId,
                                DDItemServerName = s.DDItemServerName,
                                PhysicalDataMixingDescription = s.PhysicalDataMixingDescription,
                                DDItemDatabaseName = s.DDItemDatabaseName,
                                FromDate = s.FromDate,
                                DDItemSchemaName = s.DDItemSchemaName,
                                DDItemTableName = s.DDItemTableName,
                                DDItemColumnName = s.DDItemColumnName,
                                PhysicalDataMixingRuleName = s.PhysicalDataMixingRuleName,
                                PhysicalDataMixingId = s.PhysicalDataMixingId,
                                ThruDate = s.ThruDate,
                                PhysicalDataMixingMasterId = s.PhysicalDataMixingMasterId,
                                IsActive = s.IsActive,
                                CreatedBy = s.CreatedBy,
                                ModifiedBy = s.ModifiedBy,
                                ModifiedDate = s.ModifiedDate,

                            }).Where(v => v.PhysicalDataMixingMasterId == request.PhysicalDataMixingMasterId).OrderBy(o => o.DataDictionaryItemId).ThenBy(x => x.FromDate).ToList();







                return DataSourceLoader.Load(allData, loadOptions);

            }
            else
            {
                var allData = (from s in _EahouseContext.PhysicalDataMixing
                               join pdr in _EahouseContext.PhysicalDataMixingRule
                               on s.PhysicalDataMixingRuleName equals pdr.PhysicalDataMixingRuleName
                               where s.IsActive == true && pdr.PhysicalDataMixingRuleId==request.PhysicalDataMixingRuleId
                               select new PhysicalDataMixingRequestModel
                               {
                                   DataDictionaryItemId = s.DataDictionaryItemId,
                                   DDItemServerName = s.DDItemServerName,
                                   PhysicalDataMixingDescription = s.PhysicalDataMixingDescription,
                                   DDItemDatabaseName = s.DDItemDatabaseName,
                                   FromDate = s.FromDate,
                                   DDItemSchemaName = s.DDItemSchemaName,
                                   DDItemTableName = s.DDItemTableName,
                                   DDItemColumnName = s.DDItemColumnName,
                                   PhysicalDataMixingRuleName = s.PhysicalDataMixingRuleName,
                                   PhysicalDataMixingId = s.PhysicalDataMixingId,
                                   ThruDate = s.ThruDate,
                                   PhysicalDataMixingMasterId = s.PhysicalDataMixingMasterId,
                                   IsActive = s.IsActive,
                                   CreatedBy = s.CreatedBy,
                                   ModifiedBy = s.ModifiedBy,
                                   ModifiedDate = s.ModifiedDate,

                               }).Where(v => v.PhysicalDataMixingMasterId == request.PhysicalDataMixingMasterId).OrderBy(o => o.DataDictionaryItemId).ThenBy(x => x.FromDate).ToList();







                return DataSourceLoader.Load(allData, loadOptions);


                
            }

        }

        public object GetByPhysicalDataMixingTableById(PhysicalDataMixingTableRequestModel request, DataSourceLoadOptionsBase loadOptions)
        {////
            var result0 = (from pdm in _EahouseContext.PhysicalDataMixing
                           where pdm.PhysicalDataMixingMasterId == request.PhysicalDataMixingMasterId && pdm.IsActive==true
                           select new KeyValueResponseModel()
                           {
                               Key = pdm.PhysicalDataMixingId,
                               Value = pdm.DDItemTableName,
                           }).ToList();

            var result = result0.GroupBy(u => u.Value).Select((b, index) => new KeyValueResponseModel
            {

                Key = index + 1,
                Value = b.First().Value,

            }).ToList();
            if (request.PhysicalDataMixingId == 0 || request == null || request.PhysicalDataMixingId == null)
            {
                var allData = (from s in _EahouseContext.PhysicalDataMixing
                               where s.IsActive == true
                               select new PhysicalDataMixingRequestModel
                               {
                                   DataDictionaryItemId = s.DataDictionaryItemId,
                                   DDItemServerName = s.DDItemServerName,
                                   PhysicalDataMixingDescription = s.PhysicalDataMixingDescription,
                                   DDItemDatabaseName = s.DDItemDatabaseName,
                                   FromDate = s.FromDate,
                                   DDItemSchemaName = s.DDItemSchemaName,
                                   DDItemTableName = s.DDItemTableName,
                                   DDItemColumnName = s.DDItemColumnName,
                                   PhysicalDataMixingRuleName = s.PhysicalDataMixingRuleName,
                                   PhysicalDataMixingId = s.PhysicalDataMixingId,
                                   ThruDate = s.ThruDate,
                                   PhysicalDataMixingMasterId = s.PhysicalDataMixingMasterId,
                                   IsActive = s.IsActive,
                                   CreatedBy = s.CreatedBy,
                                   ModifiedBy = s.ModifiedBy,
                                   ModifiedDate = s.ModifiedDate,

                               }).Where(v => v.PhysicalDataMixingMasterId == request.PhysicalDataMixingMasterId).OrderBy(o => o.DataDictionaryItemId).ThenBy(x => x.FromDate).ToList();







                return DataSourceLoader.Load(allData, loadOptions);

            }
            else
            {
                var allData = (from s in _EahouseContext.PhysicalDataMixing
                               join rs in result
                               on s.DDItemTableName equals rs.Value
                               where s.IsActive == true && rs.Key == request.PhysicalDataMixingId
                               select new PhysicalDataMixingRequestModel
                               {
                                   DataDictionaryItemId = s.DataDictionaryItemId,
                                   DDItemServerName = s.DDItemServerName,
                                   PhysicalDataMixingDescription = s.PhysicalDataMixingDescription,
                                   DDItemDatabaseName = s.DDItemDatabaseName,
                                   FromDate = s.FromDate,
                                   DDItemSchemaName = s.DDItemSchemaName,
                                   DDItemTableName = s.DDItemTableName,
                                   DDItemColumnName = s.DDItemColumnName,
                                   PhysicalDataMixingRuleName = s.PhysicalDataMixingRuleName,
                                   PhysicalDataMixingId = s.PhysicalDataMixingId,
                                   ThruDate = s.ThruDate,
                                   PhysicalDataMixingMasterId = s.PhysicalDataMixingMasterId,
                                   IsActive = s.IsActive,
                                   CreatedBy = s.CreatedBy,
                                   ModifiedBy = s.ModifiedBy,
                                   ModifiedDate = s.ModifiedDate,

                               }).Where(v => v.PhysicalDataMixingMasterId == request.PhysicalDataMixingMasterId).OrderBy(o => o.DataDictionaryItemId).ThenBy(x => x.FromDate).ToList();







                return DataSourceLoader.Load(allData, loadOptions);



            }

        }


    }
}