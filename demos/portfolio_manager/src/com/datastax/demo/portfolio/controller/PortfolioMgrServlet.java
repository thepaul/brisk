package com.datastax.demo.portfolio.controller;

import com.datastax.demo.portfolio.PortfolioMgr;

import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.server.TServlet;

public class PortfolioMgrServlet extends TServlet
{
    public PortfolioMgrServlet()
    {
        super(new PortfolioMgr.Processor(new PortfolioMgrHandler()), new TJSONProtocol.Factory());
    }    
}
